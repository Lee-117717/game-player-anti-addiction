#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
=============================================================================
基于Hadoop的游戏平台玩家行为分析与防沉迷系统
模拟数据生成脚本 - 模拟游戏玩家行为日志实时产生
=============================================================================
版本: 1.0
Python: 3.8+
用途: 生成符合规范的玩家行为日志，用于Flume采集 -> Kafka -> Hadoop/Hive处理
运行方式:
    python3 generate_game_logs.py
    python3 generate_game_logs.py <总条数> <间隔秒数>
    示例: python3 generate_game_logs.py 10000 0.5
=============================================================================
"""

import random
import time
import sys
import os
from datetime import datetime, timedelta

# ============================================================================
# 全局配置
# ============================================================================

# 默认生成日志条数
DEFAULT_TOTAL_RECORDS = 10000

# 默认每条日志写入间隔（秒），模拟实时产生，设为0则无间隔
DEFAULT_INTERVAL_SECONDS = 0.1

# 日志输出文件路径
LOG_OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
LOG_OUTPUT_FILE = os.path.join(LOG_OUTPUT_DIR, "game_player_behavior.log")

# 最大在线时长（秒），即6小时
MAX_ONLINE_SECONDS = 21600

# 模拟时间起点（日志中的登录时间范围从此开始）
# 动态使用当前日期 - 9天，确保模拟数据覆盖最近10天（含今天）
SIMULATION_START_DATE = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=9)

# ============================================================================
# 模拟数据池 - 用于随机取值
# ============================================================================

# 游戏ID列表（模拟多款游戏）
GAME_IDS = [
    "GAME_001", "GAME_002", "GAME_003", "GAME_004", "GAME_005",
    "GAME_006", "GAME_007", "GAME_008", "GAME_009", "GAME_010",
]

# 游戏区域列表
GAME_REGIONS = [
    "华东一区", "华东二区", "华南一区", "华南二区",
    "华北一区", "华北二区", "华中一区", "西南一区",
    "西北一区", "东北一区", "港澳台区", "海外区",
]

# 设备类型及其权重（模拟移动端用户占多数）
DEVICE_TYPES = ["android", "ios", "pc"]
DEVICE_WEIGHTS = [45, 35, 20]  # 安卓45% iOS35% PC20%

# 登录时段定义
# 白天: 06:00-17:59, 傍晚: 18:00-23:59, 夜间: 00:00-05:59
PERIOD_DAYTIME = "白天"
PERIOD_EVENING = "傍晚"
PERIOD_NIGHT = "夜间"

# 账号类型
ACCOUNT_TYPE_MINOR = "minor"   # 未成年
ACCOUNT_TYPE_ADULT = "adult"   # 成年

# ============================================================================
# 工具函数
# ============================================================================

def random_ip():
    """
    生成随机内网/公网IP地址
    大部分使用内网IP（模拟家庭/学校网络），少部分使用公网IP
    """
    ip_type = random.random()
    if ip_type < 0.7:
        # 70% 生成常见内网IP段
        prefix_pool = [
            "192.168",               # 家庭/企业内网
            "10",                    # 大型内网
            "172.{}".format(random.randint(16, 31)),  # 中型内网
        ]
        prefix = random.choice(prefix_pool)
        return "{}.{}.{}".format(prefix, random.randint(0, 255), random.randint(1, 255))
    else:
        # 30% 生成模拟公网IP
        return "{}.{}.{}.{}".format(
            random.randint(1, 223),
            random.randint(0, 255),
            random.randint(0, 255),
            random.randint(1, 255),
        )


def determine_period(login_datetime):
    """
    根据登录时间判断所属时段
    - 白天(daytime):  06:00 ~ 17:59
    - 傍晚(evening):  18:00 ~ 23:59
    - 夜间(night):    00:00 ~ 05:59
    """
    hour = login_datetime.hour
    if 6 <= hour < 18:
        return PERIOD_DAYTIME
    elif 18 <= hour <= 23:
        return PERIOD_EVENING
    else:
        return PERIOD_NIGHT


def weighted_random_login_hour(is_minor):
    """
    根据玩家类型加权随机生成登录小时（0-23），返回datetime的小时部分。

    未成年玩家特征:
      - 夜间(22:00-05:00)登录概率显著提高，模拟沉迷场景
      - 白天上课时间(08:00-16:00)登录概率较低
      - 放学后(16:00-22:00)是登录高峰期

    成年玩家特征:
      - 傍晚到夜间(18:00-01:00)登录概率较高（下班后）
      - 白天登录概率均匀分布（碎片时间游戏）
    """
    if is_minor:
        # ----------------------------------------------------------
        # 未成年人: 按时段分配权重，模拟沉迷行为
        # 权重分布（总和=100）:
        #   深夜 00:00-05:59 (6小时) → 权重 24（每小时4）
        #   上课 06:00-07:59 (2小时) → 权重  4（每小时2）
        #   上午 08:00-11:59 (4小时) → 权重  8（每小时2）
        #   下午 12:00-15:59 (4小时) → 权重 12（每小时3）
        #   放学 16:00-21:59 (6小时) → 权重 36（每小时6）← 高峰期
        #   深夜 22:00-23:59 (2小时) → 权重 16（每小时8）← 沉迷高发
        # ----------------------------------------------------------
        hour_weights = []
        for h in range(24):
            if 0 <= h <= 5:       # 深夜段
                hour_weights.append(4)
            elif 6 <= h <= 7:     # 清晨段
                hour_weights.append(2)
            elif 8 <= h <= 11:    # 上午上课段
                hour_weights.append(2)
            elif 12 <= h <= 15:   # 下午段
                hour_weights.append(3)
            elif 16 <= h <= 21:   # 放学后高峰期
                hour_weights.append(6)
            else:                 # 22-23点，沉迷高发时段
                hour_weights.append(8)
    else:
        # ----------------------------------------------------------
        # 成年人: 下班后和周末时段集中
        # 权重分布（总和=100）:
        #   深夜 00:00-07:59 (8小时) → 权重 16（每小时2）
        #   上午 08:00-11:59 (4小时) → 权重 12（每小时3）
        #   下午 12:00-17:59 (6小时) → 权重 18（每小时3）
        #   傍晚 18:00-21:59 (4小时) → 权重 28（每小时7）← 晚高峰
        #   深夜 22:00-23:59 (2小时) → 权重 16（每小时8）
        # ----------------------------------------------------------
        hour_weights = []
        for h in range(24):
            if 0 <= h <= 7:
                hour_weights.append(2)
            elif 8 <= h <= 11:
                hour_weights.append(3)
            elif 12 <= h <= 17:
                hour_weights.append(4)
            elif 18 <= h <= 21:
                hour_weights.append(7)
            else:
                hour_weights.append(8)

    # 加权随机选择小时
    hours = list(range(24))
    return random.choices(hours, weights=hour_weights, k=1)[0]


def generate_online_duration(is_minor, is_night):
    """
    生成合理的在线时长（秒）。

    规则:
      - 普通情况: 在线时长 60~7200秒 (1分钟~2小时)
      - 沉迷模拟: 未成年+夜间 → 较大概率产生长在线时长
      - 极端情况: 小概率出现超长在线(>4小时)
      - 最大不超过 MAX_ONLINE_SECONDS (21600秒，即6小时)
    """
    # 基础在线时长分布（秒），使用对数正态分布的思路
    if is_minor and is_night:
        # 未成年夜间: 更容易沉迷，在线时长偏长
        # 60%概率在30分钟~3小时之间，25%概率在3~5小时，15%概率在1~30分钟
        roll = random.random()
        if roll < 0.15:
            duration = random.randint(60, 1800)       # 1~30分钟
        elif roll < 0.75:
            duration = random.randint(1800, 10800)    # 30分钟~3小时
        elif roll < 0.95:
            duration = random.randint(10800, 18000)   # 3~5小时（沉迷）
        else:
            duration = random.randint(18000, 21600)   # 5~6小时（重度沉迷）
    else:
        # 成年人 / 未成年非夜间: 正常分布
        roll = random.random()
        if roll < 0.20:
            duration = random.randint(60, 600)        # 1~10分钟（短时）
        elif roll < 0.65:
            duration = random.randint(600, 3600)      # 10~60分钟（正常）
        elif roll < 0.90:
            duration = random.randint(3600, 10800)    # 1~3小时（较长）
        else:
            duration = random.randint(10800, 21600)   # 3~6小时（长时）

    # 确保不超过最大值
    return min(duration, MAX_ONLINE_SECONDS)


def generate_recharge_amount(is_minor, match_count):
    """
    生成合理的充值金额（单位：元）。

    规则:
      - 成年玩家消费能力更强，充值金额整体更高
      - 未成年玩家充值受到限制，偶有大额（模拟非理性消费）
      - 对局次数多的玩家有更大概率产生充值行为
      - 多数情况下为0（未充值）
    """
    # 充值概率: 基础30%，对局多的玩家概率增高
    base_chance = 0.30
    if match_count > 10:
        base_chance += 0.15
    if match_count > 20:
        base_chance += 0.10

    if random.random() > base_chance:
        return 0  # 本次未充值

    if is_minor:
        # 未成年: 小额为主，偶有大额
        roll = random.random()
        if roll < 0.60:
            return round(random.uniform(1, 30), 2)     # 1~30元
        elif roll < 0.85:
            return round(random.uniform(30, 100), 2)   # 30~100元
        elif roll < 0.95:
            return round(random.uniform(100, 300), 2)  # 100~300元
        else:
            return round(random.uniform(300, 648), 2)  # 大额（如648首充档）
    else:
        # 成年: 消费能力更强
        roll = random.random()
        if roll < 0.35:
            return round(random.uniform(1, 50), 2)      # 1~50元（月卡档）
        elif roll < 0.60:
            return round(random.uniform(50, 200), 2)    # 50~200元
        elif roll < 0.85:
            return round(random.uniform(200, 500), 2)   # 200~500元
        elif roll < 0.95:
            return round(random.uniform(500, 1000), 2)  # 500~1000元
        else:
            return round(random.uniform(1000, 5000), 2) # 氪金大佬


def generate_match_count(is_minor, online_seconds):
    """
    根据在线时长生成合理的对局次数。

    规则:
      - 假设每局游戏平均15~30分钟，在线时长越长对局越多
      - 未成年玩家可能在相同在线时长内打更多对局（反应快/沉迷）
      - 最少0局（挂机），最多根据在线时长计算
    """
    if online_seconds < 60:
        return 0  # 不足1分钟，视为登录即下线

    # 假设每局平均耗时范围（秒）
    avg_game_duration = random.randint(900, 1800)  # 15~30分钟/局

    # 理论最大对局数
    max_possible = max(1, online_seconds // avg_game_duration)

    if is_minor:
        # 未成年: 游戏密度更高
        base = max_possible * random.uniform(0.5, 1.0)
    else:
        # 成年: 可能间歇性游戏，密度稍低
        base = max_possible * random.uniform(0.3, 0.9)

    # 加入随机波动
    match_count = int(base * random.uniform(0.7, 1.3))

    # 边界处理: 至少0局，最多不超过理论最大值的120%
    match_count = max(0, min(match_count, int(max_possible * 1.2)))

    return match_count


def generate_item_consumption(is_minor, match_count, recharge_amount):
    """
    生成道具消费数量（游戏内虚拟道具消耗）。

    规则:
      - 有对局的玩家才会消耗道具
      - 充值金额高的玩家道具消耗也更大
      - 未成年玩家可能在装扮类道具上消费更多
    """
    if match_count == 0:
        return 0

    # 每局平均消耗道具数
    avg_items_per_game = random.randint(2, 8)
    base_consumption = match_count * avg_items_per_game

    # 充值玩家额外获得道具（购买所得），放大消耗
    if recharge_amount > 0:
        bonus_multiplier = 1 + (recharge_amount / 200)  # 每200元增加1倍基数
        base_consumption = int(base_consumption * bonus_multiplier)

    # 随机波动 ±30%
    actual = int(base_consumption * random.uniform(0.7, 1.3))

    return max(0, actual)


def generate_player_id(index):
    """
    生成玩家ID，格式: PLAYER_加上8位零填充序号
    示例: PLAYER_00000001
    """
    return "PLAYER_{:08d}".format(index)


# ============================================================================
# 日志记录生成
# ============================================================================

def generate_one_log_record(player_pool_size=2000):
    """
    生成一条完整的玩家行为日志记录。

    参数:
        player_pool_size: 模拟的玩家池大小，随机选择玩家ID

    返回:
        str: 用 | 分隔的日志字符串
    """
    # --- 1. 玩家ID: 从玩家池中随机选择一个已有玩家 ---
    player_idx = random.randint(1, player_pool_size)
    player_id = generate_player_id(player_idx)

    # --- 2. 账号类型: 约30%为未成年，70%为成年（模拟真实比例） ---
    is_minor = random.random() < 0.30
    account_type = ACCOUNT_TYPE_MINOR if is_minor else ACCOUNT_TYPE_ADULT

    # --- 3. 游戏ID: 随机选择一款游戏 ---
    game_id = random.choice(GAME_IDS)

    # --- 4. 登录时间: 根据玩家类型加权随机生成 ---
    login_hour = weighted_random_login_hour(is_minor)
    # 在模拟时间范围内随机选一天，加上随机的小时、分钟、秒
    days_offset = random.randint(0, 9)  # 模拟10天内的数据
    login_datetime = SIMULATION_START_DATE + timedelta(
        days=days_offset,
        hours=login_hour,
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
    )

    # --- 5. 在线时长: 根据玩家类型和登录时段生成 ---
    is_night = (determine_period(login_datetime) == PERIOD_NIGHT)
    online_seconds = generate_online_duration(is_minor, is_night)

    # --- 6. 下线时间: 登录时间 + 在线时长 ---
    logout_datetime = login_datetime + timedelta(seconds=online_seconds)

    # --- 7. 对局次数: 根据在线时长和玩家类型生成 ---
    match_count = generate_match_count(is_minor, online_seconds)

    # --- 8. 充值金额: 根据玩家类型和对局数生成 ---
    recharge_amount = generate_recharge_amount(is_minor, match_count)

    # --- 9. 道具消费: 根据对局和充值生成 ---
    item_consumption = generate_item_consumption(is_minor, match_count, recharge_amount)

    # --- 10. 登录IP: 随机生成 ---
    login_ip = random_ip()

    # --- 11. 登录时段: 根据登录时间判断 ---
    login_period = determine_period(login_datetime)

    # --- 12. 游戏区域: 随机选择 ---
    game_region = random.choice(GAME_REGIONS)

    # --- 13. 设备类型: 按权重随机选择 ---
    device_type = random.choices(DEVICE_TYPES, weights=DEVICE_WEIGHTS, k=1)[0]

    # --- 组装日志字符串，使用 | 作为分隔符（Flume常用格式） ---
    # 字段顺序:
    # 玩家ID | 账号类型 | 游戏ID | 登录时间 | 下线时间 | 在线时长(秒) |
    # 对局次数 | 充值金额(元) | 道具消费 | 登录IP | 登录时段 | 游戏区域 | 设备类型
    log_line = "|".join([
        player_id,
        account_type,
        game_id,
        login_datetime.strftime("%Y-%m-%d %H:%M:%S"),
        logout_datetime.strftime("%Y-%m-%d %H:%M:%S"),
        str(online_seconds),
        str(match_count),
        "{:.2f}".format(recharge_amount),
        str(item_consumption),
        login_ip,
        login_period,
        game_region,
        device_type,
    ])

    return log_line


# ============================================================================
# 主程序
# ============================================================================

def main():
    """
    主函数: 解析命令行参数，生成日志数据并实时写入文件。

    用法:
        python3 generate_game_logs.py                    # 默认1万条，间隔0.1秒
        python3 generate_game_logs.py <条数> <间隔秒>     # 自定义参数
        python3 generate_game_logs.py 5000 0              # 5000条，无间隔（快速生成）
    """
    # --- 解析命令行参数 ---
    total_records = DEFAULT_TOTAL_RECORDS
    interval_seconds = DEFAULT_INTERVAL_SECONDS

    if len(sys.argv) >= 2:
        try:
            total_records = int(sys.argv[1])
        except ValueError:
            print("[错误] 参数1(总条数)必须是整数，使用默认值 {}".format(DEFAULT_TOTAL_RECORDS))
            total_records = DEFAULT_TOTAL_RECORDS

    if len(sys.argv) >= 3:
        try:
            interval_seconds = float(sys.argv[2])
        except ValueError:
            print("[错误] 参数2(间隔秒数)必须是数字，使用默认值 {}".format(DEFAULT_INTERVAL_SECONDS))
            interval_seconds = DEFAULT_INTERVAL_SECONDS

    # --- 创建日志输出目录 ---
    os.makedirs(LOG_OUTPUT_DIR, exist_ok=True)

    # --- 打印运行信息 ---
    print("=" * 70)
    print("  游戏玩家行为日志模拟生成器")
    print("=" * 70)
    print("  输出文件: {}".format(LOG_OUTPUT_FILE))
    print("  生成条数: {} 条".format(total_records))
    print("  写入间隔: {} 秒/条".format(interval_seconds))
    print("  字段分隔符: | (竖线)")
    print("=" * 70)
    print("")

    # --- 记录日志格式头（写入文件第一行，方便查阅） ---
    header = (
        "# 游戏玩家行为日志 - 模拟数据\n"
        "# 生成时间: {}\n"
        "# 总记录数: {}\n"
        "# 字段说明: 玩家ID|账号类型|游戏ID|登录时间|下线时间|在线时长(秒)|"
        "对局次数|充值金额(元)|道具消费|登录IP|登录时段|游戏区域|设备类型\n"
        "# 账号类型: minor=未成年, adult=成年\n"
        "# 登录时段: 白天(06-18)|傍晚(18-24)|夜间(00-06)\n"
        "# ======================================================\n"
    ).format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), total_records)

    # --- 打开文件，准备追加写入 ---
    with open(LOG_OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(header)
        f.flush()  # 立即刷新到磁盘

    print("[信息] 日志文件头已写入: {}".format(LOG_OUTPUT_FILE))
    print("[信息] 开始生成日志数据...\n")

    # --- 统计变量 ---
    minor_count = 0      # 未成年玩家计数
    adult_count = 0      # 成年玩家计数
    night_count = 0      # 夜间登录计数
    minor_night_count = 0  # 未成年夜间登录计数
    start_time = time.time()

    # --- 逐条生成并写入日志 ---
    for i in range(1, total_records + 1):
        # 生成一条日志记录
        log_line = generate_one_log_record()

        # 追加写入文件（模拟实时日志产生，每条立即写入并刷新）
        with open(LOG_OUTPUT_FILE, "a", encoding="utf-8") as f:
            f.write(log_line + "\n")
            f.flush()  # 立即刷新，确保Flume能实时采集

        # --- 更新统计计数（从日志行中解析，不重复调用生成函数） ---
        fields = log_line.split("|")
        account_type = fields[1]
        login_period = fields[10]

        if account_type == ACCOUNT_TYPE_MINOR:
            minor_count += 1
        else:
            adult_count += 1

        if login_period == PERIOD_NIGHT:
            night_count += 1
            if account_type == ACCOUNT_TYPE_MINOR:
                minor_night_count += 1

        # --- 进度显示: 每1000条或最后一条打印进度 ---
        if i % 1000 == 0 or i == total_records:
            elapsed = time.time() - start_time
            speed = i / elapsed if elapsed > 0 else 0
            print("[进度] 已生成: {}/{} 条 | "
                  "耗时: {:.1f}秒 | "
                  "速度: {:.1f}条/秒 | "
                  "未成年: {} | 成年: {}".format(
                      i, total_records, elapsed, speed,
                      minor_count, adult_count))

        # --- 每条之间等待指定间隔，模拟实时日志产生 ---
        if interval_seconds > 0:
            time.sleep(interval_seconds)

    # --- 输出最终统计报告 ---
    total_elapsed = time.time() - start_time
    print("\n" + "=" * 70)
    print("  日志生成完成!")
    print("=" * 70)
    print("  输出文件: {}".format(LOG_OUTPUT_FILE))
    print("  总记录数: {} 条".format(total_records))
    print("  总耗时:   {:.2f} 秒".format(total_elapsed))
    print("  平均速度: {:.1f} 条/秒".format(
        total_records / total_elapsed if total_elapsed > 0 else total_records))
    print("-" * 70)
    print("  数据统计:")
    print("    成年玩家: {} 条 ({:.1f}%)".format(
        adult_count, adult_count / total_records * 100))
    print("    未成年玩家: {} 条 ({:.1f}%)".format(
        minor_count, minor_count / total_records * 100))
    print("    夜间登录: {} 条 ({:.1f}%)".format(
        night_count, night_count / total_records * 100))
    if minor_count > 0:
        print("    其中未成年夜间登录: {} 条 (占未成年的 {:.1f}%)".format(
            minor_night_count, minor_night_count / minor_count * 100))
    print("-" * 70)
    print("  文件大小: {:.2f} MB".format(
        os.path.getsize(LOG_OUTPUT_FILE) / (1024 * 1024)))
    print("=" * 70)

    # --- 打印示例日志行 ---
    print("\n[示例] 日志格式预览（前5条）:")
    print("-" * 70)
    with open(LOG_OUTPUT_FILE, "r", encoding="utf-8") as f:
        lines = f.readlines()
        # 跳过注释头，显示前5条数据
        data_lines = [l for l in lines if not l.startswith("#")]
        for idx, line in enumerate(data_lines[:5], 1):
            print("  [{}] {}".format(idx, line.rstrip()))

    print("\n[提示] 日志文件路径: {}".format(os.path.abspath(LOG_OUTPUT_FILE)))
    print("[提示] 可配置Flume的SpoolDir Source或Exec Source采集此日志文件。")


if __name__ == "__main__":
    main()
