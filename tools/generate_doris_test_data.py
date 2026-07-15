#!/usr/bin/env python3
"""
生成 Doris DWD 表 7 天模拟数据，覆盖 FineBI 全部 8 个数据集所需的场景。
直接输出 INSERT SQL，管道到 mysql 客户端执行。

用法:
    python3 generate_doris_test_data.py | mysql -h 127.0.0.1 -P 9030 -u root
"""
import random
import sys
from datetime import datetime, timedelta, date, time

random.seed(42)  # 可复现

# ============================================================
# 配置
# ============================================================
START_DATE = date(2026, 6, 11)  # 6/10 already has 8,412 rows
END_DATE   = date(2026, 6, 16)
ROWS_PER_DAY = 1500            # ~9,000 new rows total

GAME_IDS     = [f'GAME_{i:03d}' for i in range(1, 11)]
REGIONS      = ['华东一区','华东二区','华南一区','华南二区','华北一区','华北二区',
                '西南一区','西南二区','华中一区','东北一区','西北一区','海外区']
DEVICES      = ['android']*45 + ['ios']*35 + ['pc']*20
HOURS_WEIGHT = (
    [0]*4 + [1]*4 + [2]*2 + [3]*2 +           # 0-7 深夜→清晨
    [8]*3 + [9]*3 + [10]*3 + [11]*3 +          # 8-11 上午
    [12]*4 + [13]*4 + [14]*4 + [15]*4 +        # 12-15 下午
    [16]*6 + [17]*6 + [18]*6 + [19]*6 +        # 16-19 傍晚高峰
    [20]*8 + [21]*8 + [22]*6 + [23]*4           # 20-23 晚间高峰
)

# 充值金额分布 (元)
RECHARGE_DIST = (
    [0]*40 +                                    # 40% 未付费
    [random.uniform(1,30) for _ in range(30)] + # 30% 1-30元
    [random.uniform(30,100) for _ in range(15)]+# 15% 30-100元
    [random.uniform(100,300) for _ in range(10)]+# 10% 100-300元
    [random.uniform(300,1000) for _ in range(4)]+# 4% 300-1000元
    [random.uniform(1000,5000) for _ in range(1)] # 1% 1000+元
)

# 在线时长分布 (秒) → 对应 <30min, 30-60min, 1-2h, 2-3h, 3-4h, >4h
DURATION_DIST = (
    [random.randint(60, 29*60) for _ in range(18)] +   # 18% <30min(秒)
    [random.randint(30*60, 59*60) for _ in range(15)] + # 15% 30-60min
    [random.randint(60*60, 119*60) for _ in range(22)] + # 22% 1-2h
    [random.randint(120*60, 179*60) for _ in range(20)] + # 20% 2-3h
    [random.randint(180*60, 239*60) for _ in range(15)] + # 15% 3-4h
    [random.randint(240*60, 360*60) for _ in range(10)]   # 10% >4h（超限）
)

# ============================================================
# 辅助函数
# ============================================================
def gen_player_id(idx, is_minor):
    prefix = 'M' if is_minor else 'A'
    return f'{prefix}{idx:06d}'

def gen_ip():
    return f'{random.randint(1,223)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}'

def login_period(hour):
    if 0 <= hour < 6:   return '凌晨'
    if 6 <= hour < 12:  return '上午'
    if 12 <= hour < 18: return '下午'
    return '晚上'

def is_night(hour):
    return 1 if (hour >= 22 or hour < 8) else 0

def calc_risk_label(is_minor, duration_sec, night):
    if is_minor == 0:
        return 0
    # 未成年人防沉迷规则
    over_4h = duration_sec > 14400  # >4小时
    if over_4h and night:
        return 3  # 重度沉迷
    if over_4h and not night:
        return 1  # 超时预警
    if not over_4h and night:
        return 2  # 夜间违规
    return 0  # 正常

def is_heavy(duration_sec):
    return 1 if duration_sec > 14400 else 0

def is_paying(amount):
    return 1 if amount > 0 else 0

# ============================================================
# 生成 INSERT SQL
# ============================================================
print("-- ============================================================")
print("-- 游戏防沉迷系统 Doris 模拟数据 (7天)")
print(f"-- 日期范围: {START_DATE} ~ {END_DATE}, 每天 ~{ROWS_PER_DAY} 行")
print("-- ============================================================")
print()

COLUMNS = """player_id, account_type, game_id, login_time, logout_time,
login_date, login_hour, login_dayofweek, online_duration, online_duration_min,
match_count, recharge_amount, item_consumption, login_ip, login_period,
is_night_login, game_region, device_type, is_minor, is_heavy_gamer,
is_paying_player, risk_label, etl_time, source_file, dt"""

BATCH_SIZE = 100

# 为玩家池预分配属性以保持一致性
adult_pool_size = 1500
minor_pool_size = 600
total_players = adult_pool_size + minor_pool_size

adult_ids = list(range(1, adult_pool_size + 1))
minor_ids = list(range(1, minor_pool_size + 1))

current_day = START_DATE
day_idx = 0
total_rows = 0

while current_day <= END_DATE:
    day_str = current_day.strftime('%Y-%m-%d')
    day_of_week = current_day.isoweekday()
    rows_today = 0
    batch = []

    while rows_today < ROWS_PER_DAY:
        # 选择玩家类型：30% 未成年
        is_minor_flag = 1 if random.random() < 0.30 else 0

        if is_minor_flag:
            player_id = gen_player_id(random.choice(minor_ids), True)
            account_type = 'minor'
        else:
            player_id = gen_player_id(random.choice(adult_ids), False)
            account_type = 'adult'

        game_id = random.choice(GAME_IDS)
        region = random.choice(REGIONS)
        device = random.choice(DEVICES)

        # 登录小时（加权随机）
        login_hour = random.choice(HOURS_WEIGHT)
        login_min = random.randint(0, 59)
        login_sec = random.randint(0, 59)
        login_dt = datetime.combine(current_day, time(login_hour, login_min, login_sec))

        # 在线时长
        duration_sec = random.choice(DURATION_DIST)
        logout_dt = login_dt + timedelta(seconds=duration_sec)

        # 充值金额（有时重选使其分布正确）
        recharge = round(random.choice(RECHARGE_DIST), 2)
        if isinstance(recharge, float) and recharge < 0:
            recharge = 0.0

        # 衍生字段
        night = is_night(login_hour)
        risk = calc_risk_label(is_minor_flag, duration_sec, night)
        heavy = is_heavy(duration_sec)
        paying = is_paying(recharge)
        period = login_period(login_hour)
        ip = gen_ip()
        match_cnt = random.randint(0, 15)
        item_cons = random.randint(0, 200)
        duration_min = round(duration_sec / 60.0, 1)

        etl_time = login_dt + timedelta(minutes=random.randint(5, 60))
        source_file = f'/user/flume/game_logs/dt={current_day.strftime("%Y%m%d")}/game_player_behavior.{random.randint(1,20)}.log'

        values = f"""('{player_id}', '{account_type}', '{game_id}',
'{login_dt.strftime("%Y-%m-%d %H:%M:%S")}',
'{logout_dt.strftime("%Y-%m-%d %H:%M:%S")}',
'{current_day}', {login_hour}, {day_of_week},
{duration_sec}, {duration_min},
{match_cnt}, {recharge}, {item_cons},
'{ip}', '{period}',
{night}, '{region}', '{device}',
{is_minor_flag}, {heavy}, {paying},
{risk},
'{etl_time.strftime("%Y-%m-%d %H:%M:%S")}',
'{source_file}',
'{current_day}')"""

        batch.append(values)
        rows_today += 1

        if len(batch) >= BATCH_SIZE:
            print(f"INSERT INTO game_anti_addiction.dwd_game_player_behavior ({COLUMNS}) VALUES")
            print(",\n".join(batch) + ";")
            print()
            batch = []

    # 输出该日剩余批次
    if batch:
        print(f"INSERT INTO game_anti_addiction.dwd_game_player_behavior ({COLUMNS}) VALUES")
        print(",\n".join(batch) + ";")
        print()

    total_rows += rows_today
    sys.stderr.write(f"  Day {current_day}: {rows_today} rows generated\n")
    current_day += timedelta(days=1)
    day_idx += 1

sys.stderr.write(f"\nTotal: {total_rows} rows across {day_idx} days\n")
sys.stderr.write("Pipe this output to: mysql -h 127.0.0.1 -P 9030 -u root\n")
