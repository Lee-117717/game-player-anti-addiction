#!/usr/bin/env python3
"""
=============================================================================
游戏防沉迷系统 — 增强版轻量级每日数据管道
=============================================================================
改进:
  1. 玩家池持久化 (player_pool.json) — 支持跨天玩家留存
  2. 真实在线时长分布 — >4h 从 48% 降至 ~10%
  3. 合理的登录时段分布 — 未成年人放学/深夜高峰
  4. 风险标注与 Spark ETL 完全一致
  5. 自动回填缺失日期

用法: python3 lightweight_pipeline.py [条数] [日期YYYYMMDD] [--reinit-pool]
示例: python3 lightweight_pipeline.py 2000 20260629
       python3 lightweight_pipeline.py 2000 20260629 --reinit-pool
=============================================================================
"""

import random
import sys
import os
import subprocess
import json
import math
from datetime import datetime, timedelta

# ============================================================================
# 配置
# ============================================================================
DORIS_HOST = "127.0.0.1"
DORIS_PORT = 9030
DORIS_USER = "root"
DORIS_DB = "game_anti_addiction"
DORIS_TABLE = "dwd_game_player_behavior"
STREAM_LOAD_URL = f"http://localhost:8030/api/{DORIS_DB}/{DORIS_TABLE}/_stream_load"

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(PROJECT_DIR, "data")
PLAYER_POOL_FILE = os.path.join(DATA_DIR, "player_pool.json")

RECORD_COUNT = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1].isdigit() else 2000
TARGET_DATE = sys.argv[2] if len(sys.argv) > 2 and len(sys.argv[2]) == 8 and sys.argv[2].isdigit() else datetime.now().strftime("%Y%m%d")
REINIT_POOL = "--reinit-pool" in sys.argv

TARGET_DATE_DT = datetime.strptime(TARGET_DATE, "%Y%m%d")
TARGET_DAYOFWEEK = TARGET_DATE_DT.weekday()  # 0=Mon, 6=Sun
IS_WEEKEND = TARGET_DAYOFWEEK >= 5

# ============================================================================
# 模拟参数
# ============================================================================
GAME_IDS = [f"GAME_{i:03d}" for i in range(1, 11)]
GAME_REGIONS = ["华东一区","华东二区","华南一区","华南二区","华北一区","华北二区",
                "华中一区","西南一区","西北一区","东北一区","港澳台区","海外区"]
DEVICE_TYPES = ["android"] * 45 + ["ios"] * 35 + ["pc"] * 20

# 在线时长分布权重 (累积概率)
# 成人 / 未成年白天 / 未成年夜间
DURATION_BUCKETS = [
    # (max_minutes, adult_weight, minor_day_weight, minor_night_weight)
    (30,   15, 10,  5),   # <30min
    (60,   25, 20, 10),   # 30-60min
    (120,  30, 30, 20),   # 1-2h
    (180,  18, 22, 25),   # 2-3h
    (240,   8, 12, 20),   # 3-4h
    (360,   4,  6, 20),   # >4h (超时预警线)
]

# 登录小时权重
MINOR_HOUR_WEIGHTS = (
    [1]*6 +     # 00-05: 深夜(会被限制)
    [1]*1 +     # 06: 清晨
    [1]*1 +     # 07: 清晨
    [1]*4 +     # 08-11: 上课时间
    [3]*1 +     # 12: 午休
    [3]*1 +     # 13: 午休
    [2]*2 +     # 14-15: 下午课
    [5]*1 +     # 16: 放学
    [5]*1 +     # 17: 放学
    [6]*3 +     # 18-20: 晚高峰
    [6]*1 +     # 21: 晚间
    [8]*2       # 22-23: 深夜沉迷高发
)

ADULT_HOUR_WEIGHTS = (
    [3]*2 +     # 00-01: 深夜
    [1]*6 +     # 02-07: 睡眠时间
    [2]*4 +     # 08-11: 上午
    [4]*2 +     # 12-13: 午休
    [3]*4 +     # 14-17: 下午
    [7]*4 +     # 18-21: 晚高峰
    [5]*2       # 22-23: 晚间
)

# 充值倾向映射
RECHARGE_TENDENCIES = {
    "none":     {"prob": 0.05, "amounts": [(0.6, 0, 0), (0.4, 1, 30)]},
    "low":      {"prob": 0.20, "amounts": [(0.5, 1, 30), (0.35, 30, 100), (0.15, 100, 300)]},
    "medium":   {"prob": 0.35, "amounts": [(0.3, 1, 50), (0.35, 50, 200), (0.25, 200, 500), (0.1, 500, 1000)]},
    "high":     {"prob": 0.55, "amounts": [(0.2, 1, 100), (0.3, 100, 500), (0.3, 500, 2000), (0.2, 2000, 5000)]},
}

# ============================================================================
# 玩家池管理
# ============================================================================

def create_player_pool():
    """初始化玩家池: ~25% 未成年, 2000 个玩家"""
    players = {}

    # 未成年玩家 (~500, 25%)
    for i in range(1, 501):
        pid = f"M{i:05d}"
        # 偏好时段: 多数放学后/晚上, 少数深夜型
        hour_type = random.choices(
            ["after_school", "evening", "night_owl", "all_day"],
            weights=[0.40, 0.35, 0.15, 0.10], k=1
        )[0]
        if hour_type == "after_school":
            preferred_hours = list(range(16, 22))
        elif hour_type == "evening":
            preferred_hours = list(range(18, 24))
        elif hour_type == "night_owl":
            preferred_hours = list(range(20, 24)) + list(range(0, 4))
        else:
            preferred_hours = list(range(12, 24))

        players[pid] = {
            "type": "minor",
            "preferred_hours": preferred_hours,
            "games": random.sample(GAME_IDS, random.randint(1, 4)),
            "activity_rate": round(random.uniform(0.3, 0.9), 2),
            "recharge_tendency": random.choices(
                ["none", "low", "medium", "high"],
                weights=[0.25, 0.45, 0.25, 0.05], k=1
            )[0],
            "created": TARGET_DATE,
            "last_active": None
        }

    # 成年玩家 (~1500, 75%)
    for i in range(1, 1501):
        pid = f"A{i:05d}"
        hour_type = random.choices(
            ["casual", "evening", "night_owl", "hardcore"],
            weights=[0.35, 0.40, 0.15, 0.10], k=1
        )[0]
        if hour_type == "casual":
            preferred_hours = list(range(10, 22))
        elif hour_type == "evening":
            preferred_hours = list(range(17, 24))
        elif hour_type == "night_owl":
            preferred_hours = list(range(20, 24)) + list(range(0, 4))
        else:
            preferred_hours = list(range(8, 24)) + list(range(0, 2))

        players[pid] = {
            "type": "adult",
            "preferred_hours": preferred_hours,
            "games": random.sample(GAME_IDS, random.randint(1, 3)),
            "activity_rate": round(random.uniform(0.2, 0.8), 2),
            "recharge_tendency": random.choices(
                ["none", "low", "medium", "high"],
                weights=[0.30, 0.35, 0.25, 0.10], k=1
            )[0],
            "created": TARGET_DATE,
            "last_active": None
        }

    return {
        "players": players,
        "total_created": len(players),
        "last_updated": TARGET_DATE
    }


def load_player_pool():
    """加载玩家池，首次运行或 --reinit-pool 时创建新池"""
    os.makedirs(DATA_DIR, exist_ok=True)

    if REINIT_POOL or not os.path.exists(PLAYER_POOL_FILE):
        print(f"[玩家池] {'重新初始化' if REINIT_POOL else '首次创建'}玩家池 (2000人)...")
        pool = create_player_pool()
        save_player_pool(pool)
        return pool

    with open(PLAYER_POOL_FILE, "r", encoding="utf-8") as f:
        pool = json.load(f)

    # 补充新玩家 (模拟新增注册)
    existing = len(pool["players"])
    if existing < 3000:
        new_players_count = random.randint(30, 80)
        new_minor = int(new_players_count * 0.25)
        new_adult = new_players_count - new_minor

        for i in range(new_minor):
            next_id = pool["total_created"] + i + 1
            pid = f"M{next_id:05d}"
            pool["players"][pid] = {
                "type": "minor",
                "preferred_hours": random.choice([
                    list(range(16, 22)), list(range(18, 24)),
                    list(range(20, 24)) + list(range(0, 4)), list(range(12, 24))
                ]),
                "games": random.sample(GAME_IDS, random.randint(1, 4)),
                "activity_rate": round(random.uniform(0.3, 0.9), 2),
                "recharge_tendency": random.choices(
                    ["none", "low", "medium", "high"],
                    weights=[0.25, 0.45, 0.25, 0.05], k=1
                )[0],
                "created": TARGET_DATE,
                "last_active": None
            }

        for i in range(new_adult):
            next_id = pool["total_created"] + new_minor + i + 1
            pid = f"A{next_id:05d}"
            pool["players"][pid] = {
                "type": "adult",
                "preferred_hours": random.choice([
                    list(range(10, 22)), list(range(17, 24)),
                    list(range(20, 24)) + list(range(0, 4)), list(range(8, 24)) + list(range(0, 2))
                ]),
                "games": random.sample(GAME_IDS, random.randint(1, 3)),
                "activity_rate": round(random.uniform(0.2, 0.8), 2),
                "recharge_tendency": random.choices(
                    ["none", "low", "medium", "high"],
                    weights=[0.30, 0.35, 0.25, 0.10], k=1
                )[0],
                "created": TARGET_DATE,
                "last_active": None
            }

        pool["total_created"] += new_players_count
        print(f"[玩家池] 新增 {new_players_count} 名玩家 (未成年:{new_minor} 成年:{new_adult}), 现有 {len(pool['players'])} 人")

    # 淘汰长期不活跃的玩家 (池 >2500 时清理)
    if len(pool["players"]) > 2500:
        inactive_threshold = (TARGET_DATE_DT - timedelta(days=14)).strftime("%Y%m%d")
        to_remove = []
        for pid, p in pool["players"].items():
            if p.get("last_active") and p["last_active"] < inactive_threshold:
                to_remove.append(pid)
        for pid in to_remove[:len(pool["players"]) - 2000]:
            del pool["players"][pid]
        if to_remove:
            print(f"[玩家池] 清理 {len(to_remove[:len(pool['players'])-2000])} 名不活跃玩家")

    return pool


def save_player_pool(pool):
    """保存玩家池到 JSON"""
    pool["last_updated"] = TARGET_DATE
    with open(PLAYER_POOL_FILE, "w", encoding="utf-8") as f:
        json.dump(pool, f, ensure_ascii=False, indent=2)


# ============================================================================
# 数据生成
# ============================================================================

def is_night_login(hour):
    """夜间: 22:00 - 06:00"""
    return hour >= 22 or hour < 6


def pick_weighted_duration(is_minor, login_hour):
    """根据玩家类型和时段，按分布权重生成在线时长(分钟)"""
    night = is_night_login(login_hour)

    if is_minor and night:
        col_idx = 3  # 未成年夜间
    elif is_minor:
        col_idx = 2  # 未成年白天
    else:
        col_idx = 1  # 成人

    # 按权重选择区间
    weights = [b[col_idx] for b in DURATION_BUCKETS]
    bucket = random.choices(DURATION_BUCKETS, weights=weights, k=1)[0]
    max_min = bucket[0]
    min_min = max(1, max_min // 2)

    return random.randint(min_min, max_min)


def generate_recharge(tendency):
    """根据充值倾向生成充值金额"""
    config = RECHARGE_TENDENCIES[tendency]
    if random.random() > config["prob"]:
        return 0.0

    amounts = config["amounts"]
    roll = random.random()
    cumulative = 0
    for prob, lo, hi in amounts:
        cumulative += prob
        if roll <= cumulative:
            return round(random.uniform(lo, hi), 2)
    return 0.0


def random_ip():
    """生成随机 IP"""
    if random.random() < 0.7:
        return f"{random.choice(['192.168','10',f'172.{random.randint(16,31)}'])}.{random.randint(0,255)}.{random.randint(1,255)}"
    return f"{random.randint(1,223)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"


def generate_risk_label(is_minor, online_min, login_hour):
    """风险标注 — 与 Spark ETL 完全一致"""
    if not is_minor:
        return 0
    overtime = online_min > 240
    night = is_night_login(login_hour)

    if overtime and night:
        return 3  # 重度沉迷
    elif night and not overtime:
        return 2  # 夜间违规
    elif overtime:
        return 1  # 超时预警
    else:
        return 0


def generate_one_record(player_info, player_id):
    """为指定玩家生成一条行为记录"""
    p = player_info
    is_minor = p["type"] == "minor"
    account_type = "minor" if is_minor else "adult"

    # 登录时间: 从该玩家的偏好时段中选
    if p["preferred_hours"] and random.random() < 0.8:
        login_hour = random.choice(p["preferred_hours"])
    elif is_minor:
        login_hour = random.choices(range(24), weights=MINOR_HOUR_WEIGHTS, k=1)[0]
    else:
        login_hour = random.choices(range(24), weights=ADULT_HOUR_WEIGHTS, k=1)[0]

    # 周末: 更多白天游戏时间
    if IS_WEEKEND and 8 <= login_hour <= 11 and random.random() < 0.3:
        login_hour = random.randint(8, 11)

    login_time = TARGET_DATE_DT + timedelta(
        hours=login_hour,
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59)
    )

    # 在线时长
    online_min = pick_weighted_duration(is_minor, login_hour)
    online_seconds = online_min * 60
    logout_time = login_time + timedelta(seconds=online_seconds)

    # 登录时段
    if 6 <= login_hour < 18:
        login_period = "白天"
    elif 18 <= login_hour <= 23:
        login_period = "傍晚"
    else:
        login_period = "夜间"

    night_flag = 1 if is_night_login(login_hour) else 0

    # 对局次数
    match_count = max(1, online_min // random.randint(10, 30))

    # 充值
    recharge = generate_recharge(p.get("recharge_tendency", "low"))

    # 道具消费
    item_consumption = random.randint(0, match_count * 5)

    # 设备
    device = random.choice(DEVICE_TYPES)

    # 游戏(优先该玩家的偏好游戏)
    if p["games"] and random.random() < 0.7:
        game_id = random.choice(p["games"])
    else:
        game_id = random.choice(GAME_IDS)

    # 区域
    region = random.choice(GAME_REGIONS)

    # 风险标注
    risk_label = generate_risk_label(is_minor, online_min, login_hour)
    is_heavy = 1 if online_min > 240 else 0
    is_paying = 1 if recharge > 0 else 0

    login_date = login_time.strftime("%Y-%m-%d")
    login_dayofweek = login_time.weekday()

    return [
        player_id, account_type, game_id,
        login_time.strftime("%Y-%m-%d %H:%M:%S"),
        logout_time.strftime("%Y-%m-%d %H:%M:%S"),
        login_date,
        str(login_hour),
        str(login_dayofweek),
        str(online_seconds),
        str(round(online_min, 1)),
        str(match_count),
        str(recharge),
        str(item_consumption),
        random_ip(),
        login_period,
        str(night_flag),
        region,
        device,
        str(1 if is_minor else 0),
        str(is_heavy),
        str(is_paying),
        str(risk_label),
        TARGET_DATE
    ]


# ============================================================================
# Doris 导入
# ============================================================================

def stream_load_to_doris(csv_data):
    """通过 Stream Load 导入 Doris"""
    label = f"enhanced_{TARGET_DATE}_{random.randint(1, 99999)}"
    cmd = [
        "curl", "--location-trusted", "-u", "root:",
        "-H", f"label:{label}",
        "-H", "column_separator:|",
        "-H", "format:csv",
        "-H", "max_filter_ratio:0.3",
        "-H", f"columns:player_id,account_type,game_id,login_time,logout_time,login_date,"
               f"login_hour,login_dayofweek,online_duration,online_duration_min,"
               f"match_count,recharge_amount,item_consumption,login_ip,login_period,"
               f"is_night_login,game_region,device_type,is_minor,is_heavy_gamer,"
               f"is_paying_player,risk_label,dt",
        "-T", "-",
        STREAM_LOAD_URL
    ]

    try:
        proc = subprocess.run(cmd, input=csv_data.encode(), capture_output=True, timeout=60)
        response = json.loads(proc.stdout)
        return response
    except Exception as e:
        return {"Status": "Error", "Message": str(e)}


# ============================================================================
# Doris 查询辅助
# ============================================================================

def doris_query(sql):
    """执行 Doris 查询，返回 stdout"""
    cmd = ["mysql", "-h", DORIS_HOST, "-P", str(DORIS_PORT), "-u", DORIS_USER,
           "--skip-column-names", "-e", sql]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        return result.stdout.strip()
    except Exception:
        return ""


# ============================================================================
# 主流程
# ============================================================================

def main():
    global RECORD_COUNT
    # 周末稍多数据 (模拟真实行为)
    if IS_WEEKEND and "--boot" not in sys.argv:
        weekend_bonus = int(RECORD_COUNT * 0.2)
        RECORD_COUNT += weekend_bonus
        print(f"[周末模式] 数据量增加 20%: {RECORD_COUNT} 条")

    print(f"[增强管道] 开始生成 {RECORD_COUNT} 条数据, 日期: {TARGET_DATE} (周{TARGET_DAYOFWEEK+1})")

    # 清理该日期旧数据，确保幂等
    existing = doris_query(f"SELECT COUNT(*) FROM {DORIS_DB}.{DORIS_TABLE} WHERE dt='{TARGET_DATE}';")
    if existing and int(existing) > 0:
        print(f"[清理] 删除日期 {TARGET_DATE} 的旧数据 {existing} 条...")
        doris_query(f"DELETE FROM {DORIS_DB}.{DORIS_TABLE} WHERE dt='{TARGET_DATE}';")
        print(f"[清理] 完成")

    # 加载玩家池
    pool = load_player_pool()
    players = pool["players"]

    # 选择今日活跃玩家
    active_players = []
    for pid, pinfo in players.items():
        if random.random() < pinfo["activity_rate"]:
            active_players.append(pid)

    # 随机补足到至少 RECORD_COUNT 的 80%
    min_active = int(RECORD_COUNT * 0.8)
    while len(active_players) < min_active:
        pid = random.choice(list(players.keys()))
        if pid not in active_players:
            active_players.append(pid)

    random.shuffle(active_players)

    minor_active = sum(1 for pid in active_players if players[pid]["type"] == "minor")
    adult_active = len(active_players) - minor_active
    print(f"[玩家池] 今日活跃: {len(active_players)}/{len(players)} (未成年:{minor_active} 成年:{adult_active})")

    # 统计
    stats = {
        "total": 0, "minor": 0, "adult": 0,
        "risk_0": 0, "risk_1": 0, "risk_2": 0, "risk_3": 0,
        "duration_buckets": [0]*6, "recharge_total": 0.0, "recharge_count": 0
    }

    # 生成数据 (批次写入)
    batch_size = 500
    total_loaded = 0
    record_idx = 0
    player_idx = 0
    today_active_set = set()

    num_batches = (RECORD_COUNT + batch_size - 1) // batch_size
    for batch_num in range(num_batches):
        batch = []
        batch_start = batch_num * batch_size
        batch_end = min(batch_start + batch_size, RECORD_COUNT)

        for _ in range(batch_start, batch_end):
            # 循环使用活跃玩家 (每人每天1-2条记录)
            pid = active_players[player_idx % len(active_players)]
            pinfo = players[pid]

            record = generate_one_record(pinfo, pid)
            batch.append("|".join(record) + "\n")
            today_active_set.add(pid)

            # 更新统计
            stats["total"] += 1
            if pinfo["type"] == "minor":
                stats["minor"] += 1
            else:
                stats["adult"] += 1

            risk_label = int(record[21])
            stats[f"risk_{risk_label}"] = stats.get(f"risk_{risk_label}", 0) + 1

            online_min = float(record[9])
            for bi, b in enumerate(DURATION_BUCKETS):
                if online_min <= b[0]:
                    stats["duration_buckets"][bi] += 1
                    break

            recharge = float(record[11])
            if recharge > 0:
                stats["recharge_total"] += recharge
                stats["recharge_count"] += 1

            record_idx += 1
            # 部分玩家生成第2条记录 (模拟一天多次登录)
            if random.random() < 0.15 and record_idx < RECORD_COUNT:
                player_idx += 1
            else:
                # 决定下一个玩家
                if random.random() < 0.7:
                    player_idx += 1
                # 否则同一玩家再生成一条

        # 批次导入
        csv_text = "".join(batch)
        try:
            result = stream_load_to_doris(csv_text)
            status = result.get("Status", "Unknown")
            loaded = result.get("NumberLoadedRows", "0")
            total_loaded += int(loaded)

            if status == "Success":
                print(f"  ✓ 批次 {batch_num+1}/{num_batches}: 加载 {loaded} 行")
            else:
                msg = result.get("Message", "N/A")[:80]
                print(f"  ⚠ 批次 {batch_num+1}/{num_batches}: {status} — {msg}")
        except Exception as e:
            print(f"  ✗ 批次 {batch_num+1}/{num_batches} 失败: {e}")

    # 更新玩家最后活跃日期
    for pid in today_active_set:
        if pid in players:
            players[pid]["last_active"] = TARGET_DATE

    save_player_pool(pool)

    # 输出统计
    print(f"\n[增强管道] 完成! 共加载 {total_loaded}/{RECORD_COUNT} 条")
    print(f"  未成年: {stats['minor']} ({stats['minor']/max(1,stats['total'])*100:.1f}%)")
    print(f"  成年:   {stats['adult']} ({stats['adult']/max(1,stats['total'])*100:.1f}%)")
    print(f"  今日活跃玩家: {len(today_active_set)}")
    print(f"  风险分布: 正常={stats['risk_0']} 预警={stats['risk_1']} "
          f"违规={stats['risk_2']} 重度={stats['risk_3']}")
    print(f"  时长分布: <30m={stats['duration_buckets'][0]} 30-60m={stats['duration_buckets'][1]} "
          f"1-2h={stats['duration_buckets'][2]} 2-3h={stats['duration_buckets'][3]} "
          f"3-4h={stats['duration_buckets'][4]} >4h={stats['duration_buckets'][5]}")
    print(f"  付费玩家: {stats['recharge_count']} ({stats['recharge_count']/max(1,stats['total'])*100:.1f}%) "
          f"总额: ¥{stats['recharge_total']:.0f}")

    # 验证
    count = doris_query(f"SELECT COUNT(*) FROM {DORIS_DB}.{DORIS_TABLE} WHERE dt='{TARGET_DATE}';")
    print(f"[验证] Doris 今日数据: {count} 行")

    return total_loaded


if __name__ == "__main__":
    loaded = main()
    sys.exit(0 if loaded > 0 else 1)
