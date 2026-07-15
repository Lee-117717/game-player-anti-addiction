#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
=============================================================================
 游戏平台玩家行为分析与防沉迷系统
 PySpark 3.1.x 数据清洗与防沉迷风险标注ETL脚本
=============================================================================
 功能模块:
   1. 读取Hive ODS层原始玩家行为日志
   2. 数据清洗: 空值过滤、异常值过滤、重复数据去重、非法IP过滤
   3. 时间格式标准化
   4. 沉迷风险标签生成 (0-3级)
   5. 统计分析: 日活、时长分布、付费率、风险标签分布
   6. 结果写入Hive DWD层明细表 + 统计结果表

 运行方式:
   spark-submit --master yarn --deploy-mode client spark/etl_anti_addiction.py 20260610
   spark-submit --master yarn --deploy-mode cluster spark/etl_anti_addiction.py 20260610

 参数说明:
   位置参数1: 处理日期，格式 yyyyMMdd，如 20260610 (必填)
   位置参数2: ODS库名，默认 game_anti_addiction (可选)
   位置参数3: DWD库名，默认 game_anti_addiction (可选)
=============================================================================
"""

import sys
import re
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType,
    IntegerType, TimestampType, DateType, BooleanType
)
from pyspark.sql.window import Window

# ============================================================================
# 全局常量配置
# ============================================================================

# 在线时长合法范围 (秒)
MIN_ONLINE_SECONDS = 0
MAX_ONLINE_SECONDS = 21600  # 6小时

# 防沉迷规则阈值
# 未成年单日累计在线时长阈值 (秒)，4小时 = 14400秒
MINOR_DAILY_LIMIT_SECONDS = 14400
# 夜间时段定义: 22:00 ~ 次日08:00
NIGHT_START_HOUR = 22
NIGHT_END_HOUR = 8

# 风险标签定义
RISK_LABEL_NORMAL = 0           # 正常玩家
RISK_LABEL_LEVEL1_WARNING = 1   # 一级预警: 未成年单日在线>4小时
RISK_LABEL_LEVEL2_VIOLATION = 2 # 二级违规: 未成年夜间(22:00-08:00)登录
RISK_LABEL_SEVERE = 3           # 重度沉迷: 同时满足以上两条

# IP地址合法性校验正则
# 合法IPv4: 0.0.0.0 ~ 255.255.255.255，不含保留/特殊段
IP_PATTERN = re.compile(
    r'^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}'
    r'(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$'
)

# 非法/保留IP段 (用于过滤)
ILLEGAL_IP_PREFIXES = [
    '0.',          # 0.x.x.x 保留地址
    '127.',        # 本地回环
    '169.254.',    # 链路本地地址
    '224.', '225.', '226.', '227.', '228.', '229.',
    '230.', '231.', '232.', '233.', '234.', '235.',
    '236.', '237.', '238.', '239.',  # D类组播
    '240.', '241.', '242.', '243.', '244.', '245.',
    '246.', '247.', '248.', '249.', '250.', '251.',
    '252.', '253.', '254.', '255.',  # E类保留
]

# Hive表名
ODS_TABLE = "ods_game_player_behavior"
DWD_TABLE = "dwd_game_player_behavior"
STATS_TABLE = "ads_anti_addiction_daily_stats"


# ============================================================================
# 工具函数
# ============================================================================

def is_valid_ip(ip_str):
    """
    校验IP地址合法性。

    条件:
      1. 非空
      2. 符合IPv4格式 (x.x.x.x, 每段0-255)
      3. 不在非法/保留IP段内

    返回: True=合法, False=非法
    """
    if not ip_str or ip_str.strip() == '':
        return False
    ip_str = ip_str.strip()
    if not IP_PATTERN.match(ip_str):
        return False
    # 检查是否以非法前缀开头
    for prefix in ILLEGAL_IP_PREFIXES:
        if ip_str.startswith(prefix):
            return False
    return True


def compute_risk_label(is_minor, online_duration, login_hour):
    """
    按照国家防沉迷规则计算沉迷风险标签。

    规则说明 (依据国家新闻出版署《关于防止未成年人沉迷网络游戏的通知》):
      - 标签0 正常玩家: 成年玩家，或未成年但无风险行为
      - 标签1 一级预警: 未成年 AND 单日在线时长 > 4小时 (14400秒)
                        但不涉及夜间登录 → 需监护人关注
      - 标签2 二级违规: 未成年 AND 夜间(22:00~08:00)登录
                        不满足时长超限 → 游戏厂商违规(未执行宵禁)
      - 标签3 重度沉迷: 未成年 AND 在线超4小时 AND 夜间登录
                        → 严重违规，需立即干预

    参数:
        is_minor (int): 1=未成年, 0=成年
        online_duration (int): 在线时长(秒)
        login_hour (int): 登录小时 (0-23)

    返回:
        int: 风险标签 0/1/2/3
    """
    # 成年玩家 → 直接归为正常
    if is_minor == 0:
        return RISK_LABEL_NORMAL

    # --- 未成年玩家风险判断 ---
    # 条件A: 在线时长超过4小时
    exceeds_daily_limit = (online_duration is not None
                           and online_duration > MINOR_DAILY_LIMIT_SECONDS)

    # 条件B: 夜间登录 (22:00 ~ 次日08:00)
    is_night = (login_hour is not None
                and (login_hour >= NIGHT_START_HOUR
                     or login_hour < NIGHT_END_HOUR))

    # 同时满足两条 → 重度沉迷
    if exceeds_daily_limit and is_night:
        return RISK_LABEL_SEVERE
    # 仅满足时长超限
    elif exceeds_daily_limit:
        return RISK_LABEL_LEVEL1_WARNING
    # 仅满足夜间登录
    elif is_night:
        return RISK_LABEL_LEVEL2_VIOLATION
    # 未成年但无风险行为
    else:
        return RISK_LABEL_NORMAL


# ============================================================================
# SparkSession 初始化
# ============================================================================

def create_spark_session(app_name="GameAntiAddiction_ETL"):
    """
    创建并配置SparkSession，启用Hive支持，适配YARN运行模式。

    返回:
        SparkSession: 配置完成的Spark会话对象
    """
    spark = (SparkSession.builder
             .appName(app_name)
             # 启用Hive支持，读取Hive表数据
             .enableHiveSupport()
             # ---- YARN 模式配置 ----
             .config("spark.sql.adaptive.enabled", "true")           # AQE自适应查询优化
             .config("spark.sql.adaptive.coalescePartitions.enabled", "true")  # 动态合并小分区
             .config("spark.sql.adaptive.skewJoin.enabled", "true")  # 自动处理数据倾斜
             .config("spark.sql.adaptive.localShuffleReader.enabled", "true")
             # 动态分区写入
             .config("hive.exec.dynamic.partition", "true")
             .config("hive.exec.dynamic.partition.mode", "nonstrict")
             # ORC写入压缩
             .config("spark.sql.orc.compression.codec", "snappy")
             # YARN调度优化
             .config("spark.scheduler.mode", "FAIR")
             .config("spark.sql.autoBroadcastJoinThreshold", "104857600")  # 100MB
             .getOrCreate())

    # 设置日志级别 (INFO 适合生产，DEBUG 适合排查问题)
    spark.sparkContext.setLogLevel("WARN")

    return spark


# ============================================================================
# 模块一: 数据读取 - 从Hive ODS层读取原始数据
# ============================================================================

def read_ods_data(spark: SparkSession, dt: str, ods_db: str) -> DataFrame:
    """
    从Hive ODS层读取指定日期的原始玩家行为日志。

    参数:
        spark: SparkSession
        dt: 分区日期，格式 yyyyMMdd (如 20260610)
        ods_db: ODS层数据库名

    返回:
        DataFrame: ODS原始数据，仅包含目标分区
    """
    ods_full_name = f"{ods_db}.{ODS_TABLE}"

    print(f"[信息] 读取ODS数据: {ods_full_name}, 分区: dt={dt}")

    # 读取ODS表并按分区过滤
    df_raw = (spark.table(ods_full_name)
              .filter(F.col("dt") == dt))

    # 记录读取的原始数据量
    raw_count = df_raw.count()
    print(f"[信息] ODS原始数据量: {raw_count} 条")

    # 预览前10行数据 (调试用)
    if raw_count > 0:
        print("[信息] ODS数据预览 (前5行):")
        df_raw.show(5, truncate=False)

    return df_raw


# ============================================================================
# 模块二: 数据清洗
# ============================================================================

def clean_data(df_raw: DataFrame) -> DataFrame:
    """
    对ODS原始数据进行多维度清洗。

    清洗规则:
      1. 过滤关键字段为空(NULL或空字符串)的记录
      2. 过滤在线时长为负数或超过21600秒(6小时)的异常记录
      3. 过滤登录时间晚于下线时间的逻辑错误
      4. 过滤IP地址不合法的记录
      5. 去除完全重复的数据行

    参数:
        df_raw: ODS原始DataFrame (全STRING类型)

    返回:
        DataFrame: 清洗后的DataFrame
    """
    total_before = df_raw.count()
    print(f"\n[清洗] 开始数据清洗，清洗前总行数: {total_before}")

    # ---- 2.1 关键字段非空过滤 ----
    # 玩家ID、登录时间、下线时间、在线时长为强制非空字段
    required_cols = ["player_id", "login_time", "logout_time", "online_duration"]
    condition_not_null = F.lit(True)
    for col_name in required_cols:
        condition_not_null = (condition_not_null
                              & F.col(col_name).isNotNull()
                              & (F.trim(F.col(col_name)) != ""))

    df_clean = df_raw.filter(condition_not_null)
    after_null_filter = df_clean.count()
    print(f"[清洗] 空值过滤后: {after_null_filter} 条 "
          f"(剔除 {total_before - after_null_filter} 条)")

    # ---- 2.2 在线时长合法性过滤 ----
    # 将online_duration转为数值并检查范围: 0 ~ 21600秒(6小时)
    df_clean = (df_clean
                .withColumn("online_duration_int",
                            F.col("online_duration").cast("int"))
                .filter((F.col("online_duration_int") >= MIN_ONLINE_SECONDS)
                        & (F.col("online_duration_int") <= MAX_ONLINE_SECONDS)))
    after_duration_filter = df_clean.count()
    print(f"[清洗] 时长过滤后: {after_duration_filter} 条 "
          f"(剔除 {after_null_filter - after_duration_filter} 条)")

    # ---- 2.3 时间逻辑过滤 ----
    # 登录时间必须严格早于下线时间
    # 先将时间字符串转为Timestamp进行比对
    df_clean = (df_clean
                .withColumn("login_ts",
                            F.to_timestamp(F.col("login_time"), "yyyy-MM-dd HH:mm:ss"))
                .withColumn("logout_ts",
                            F.to_timestamp(F.col("logout_time"), "yyyy-MM-dd HH:mm:ss"))
                .filter(F.col("login_ts") < F.col("logout_ts")))
    after_time_filter = df_clean.count()
    print(f"[清洗] 时间逻辑过滤后: {after_time_filter} 条 "
          f"(剔除 {after_duration_filter - after_time_filter} 条)")

    # ---- 2.4 IP地址合法性过滤 ----
    # 注册UDF进行IP校验 (PySpark UDF)
    is_valid_ip_udf = F.udf(is_valid_ip, BooleanType())
    df_clean = df_clean.filter(is_valid_ip_udf(F.col("login_ip")))
    after_ip_filter = df_clean.count()
    print(f"[清洗] IP过滤后: {after_ip_filter} 条 "
          f"(剔除 {after_time_filter - after_ip_filter} 条)")

    # ---- 2.5 重复数据去重 ----
    # 按所有业务字段去重 (保留第一条)
    # 注意: 排除刚添加的临时列 (online_duration_int, login_ts, logout_ts 是每行唯一的)
    business_cols = [c for c in df_clean.columns
                     if c not in ("online_duration_int", "login_ts", "logout_ts", "dt")]
    df_clean = df_clean.dropDuplicates(business_cols)
    after_dedup = df_clean.count()
    print(f"[清洗] 去重后: {after_dedup} 条 "
          f"(剔除 {after_ip_filter - after_dedup} 条)")

    total_removed = total_before - after_dedup
    print(f"[清洗] 清洗完成! 最终保留: {after_dedup} 条 "
          f"(总剔除: {total_removed} 条, 剔除率: {total_removed/total_before*100:.2f}%)")

    return df_clean


# ============================================================================
# 模块三: 时间字段标准化 + 数据格式统一
# ============================================================================

def standardize_data(df_clean: DataFrame) -> DataFrame:
    """
    统一数据类型，将ODS的STRING字段转为规范的Hive DWD类型。

    转换规则:
      - 时间字段: STRING → TIMESTAMP / DATE / INT派生
      - 数值字段: STRING → BIGINT / DOUBLE / INT
      - 标识字段: STRING → 保留 (类别型)
      - 派生字段: login_date, login_hour, login_dayofweek 等

    参数:
        df_clean: 清洗后DataFrame (含login_ts, logout_ts临时列)

    返回:
        DataFrame: 类型标准化的DataFrame
    """
    print("\n[标准化] 开始时间与字段类型标准化...")

    df_std = (df_clean
              # ---- 账号类型标准化: 统一小写 ----
              .withColumn("account_type_std",
                          F.lower(F.trim(F.col("account_type"))))

              # ---- 游戏ID标准化: 去空格，统一大写 ----
              .withColumn("game_id_std",
                          F.upper(F.trim(F.col("game_id"))))

              # ---- 时间字段: 利用已转换的 login_ts / logout_ts ----
              # login_time → TIMESTAMP (已在清洗阶段转换)
              .withColumn("login_time_std", F.col("login_ts"))
              # logout_time → TIMESTAMP
              .withColumn("logout_time_std", F.col("logout_ts"))
              # login_date → DATE (派生)
              .withColumn("login_date_std",
                          F.to_date(F.col("login_ts")))
              # login_hour → INT (派生)
              .withColumn("login_hour_std",
                          F.hour(F.col("login_ts")).cast("int"))
              # login_dayofweek → INT (派生, 1=周一 ~ 7=周日)
              .withColumn("login_dayofweek_std",
                          F.dayofweek(F.col("login_ts")).cast("int"))

              # ---- 数值字段: STRING → 数值 ----
              .withColumn("online_duration_std",
                          F.col("online_duration_int").cast("bigint"))
              .withColumn("online_duration_min_std",
                          F.round(F.col("online_duration_int") / 60.0, 1))
              .withColumn("match_count_std",
                          F.col("match_count").cast("int"))
              .withColumn("recharge_amount_std",
                          F.col("recharge_amount").cast("double"))
              .withColumn("item_consumption_std",
                          F.col("item_consumption").cast("bigint"))

              # ---- 属性字段保留 ----
              .withColumn("login_ip_std", F.trim(F.col("login_ip")))
              .withColumn("login_period_std", F.trim(F.col("login_period")))
              .withColumn("game_region_std", F.trim(F.col("game_region")))
              .withColumn("device_type_std",
                          F.lower(F.trim(F.col("device_type"))))

              # ---- 防沉迷基础标识字段 ----
              .withColumn("is_minor_std",
                          F.when(F.col("account_type_std") == "minor", 1).otherwise(0))
              .withColumn("is_night_login_std",
                          F.when(F.col("login_period_std") == "夜间", 1).otherwise(0))
              .withColumn("is_paying_player_std",
                          F.when(F.col("recharge_amount_std") > 0, 1).otherwise(0))

              # ---- 数据溯源 ----
              .withColumn("etl_time_std", F.current_timestamp())
              .withColumn("source_file_std",
                          F.input_file_name()))

    print("[标准化] 字段类型标准化完成。")
    return df_std


# ============================================================================
# 模块四: 沉迷风险标签生成
# ============================================================================

def add_risk_labels(df_std):
    """
    按照国家防沉迷规则，为每条玩家行为记录生成沉迷风险标签。

    标签规则:
      标签0 正常玩家:
        - 成年玩家
        - 未成年但: 在线≤4小时 且 不在夜间(22:00-08:00)登录

      标签1 一级预警 (单日在线超限):
        - 未成年 AND 单日在线 > 4小时(14400秒) AND 非夜间登录
        → 游戏公司应发送提醒给监护人

      标签2 二级违规 (夜间违规登录):
        - 未成年 AND 夜间(22:00~次日08:00)登录 AND 单日在线 ≤ 4小时
        → 游戏公司未严格执行未成年宵禁，需整改

      标签3 重度沉迷:
        - 未成年 AND 单日在线 > 4小时 AND 夜间登录
        → 严重违规，需立即强制下线并通知监护人

    关于"夜间"的判定:
      防沉迷政策定义: 每日22时至次日8时不得向未成年人提供游戏服务
      因此以 login_hour 判定: (login_hour >= 22) 或 (login_hour < 8)

    参数:
        df_std: 标准化后的DataFrame

    返回:
        DataFrame: 新增 risk_label 字段的DataFrame
    """
    print("\n[风险标注] 开始生成防沉迷风险标签...")

    # 注册风险标签计算UDF
    risk_label_udf = F.udf(compute_risk_label, IntegerType())

    df_labeled = df_std.withColumn(
        "risk_label",
        risk_label_udf(
            F.col("is_minor_std"),
            F.col("online_duration_std"),
            F.col("login_hour_std")
        )
    )

    # 统计风险标签分布
    label_counts = (df_labeled
                    .groupBy("risk_label")
                    .count()
                    .orderBy("risk_label")
                    .collect())

    print("[风险标注] 风险标签分布:")
    label_names = {
        0: "正常玩家",
        1: "一级预警(单日>4小时)",
        2: "二级违规(夜间登录)",
        3: "重度沉迷(时长+夜间)"
    }
    total = df_labeled.count()
    for row in label_counts:
        name = label_names.get(row["risk_label"], f"未知({row['risk_label']})")
        pct = row["count"] / total * 100 if total > 0 else 0
        print(f"  标签{row['risk_label']} - {name}: {row['count']} 条 ({pct:.1f}%)")

    # 特别关注: 未成年风险总数
    minor_risk = (df_labeled
                  .filter((F.col("is_minor_std") == 1) & (F.col("risk_label") > 0))
                  .count())
    minor_total = df_labeled.filter(F.col("is_minor_std") == 1).count()
    if minor_total > 0:
        print(f"[风险标注] 未成年玩家总数: {minor_total}, "
              f"有风险行为: {minor_risk} ({minor_risk/minor_total*100:.1f}%)")

    return df_labeled


# ============================================================================
# 模块五: 基础指标统计
# ============================================================================

def compute_statistics(df_labeled: DataFrame, dt: str, spark: SparkSession):
    """
    计算每日基础统计指标并写入统计结果表。

    统计指标:
      1. 每日活跃玩家数
      2. 在线时长区间分布 (0-1h, 1-2h, 2-3h, 3-4h, >4h)
      3. 付费玩家数及付费率
      4. 各风险标签人数分布
      5. 未成年玩家专项统计

    参数:
        df_labeled: 带风险标签的DataFrame
        dt: 统计日期
        spark: SparkSession
    """
    print(f"\n[统计] 开始计算日统计指标 (dt={dt})...")

    # ---- 5.1 每日活跃玩家数 ----
    dau = df_labeled.select("player_id").distinct().count()
    total_records = df_labeled.count()
    print(f"[统计] 日活玩家(DAU): {dau}")
    print(f"[统计] 总行为记录数: {total_records}")

    # ---- 5.2 在线时长区间分布 ----
    print("\n[统计] 在线时长区间分布:")
    duration_stats = (df_labeled
                      .withColumn("duration_range",
                                  F.when(F.col("online_duration_std") <= 3600, "0-1小时")     # ≤1h
                                  .when(F.col("online_duration_std") <= 7200, "1-2小时")      # ≤2h
                                  .when(F.col("online_duration_std") <= 10800, "2-3小时")     # ≤3h
                                  .when(F.col("online_duration_std") <= 14400, "3-4小时")     # ≤4h
                                  .otherwise("4小时以上"))
                      .groupBy("duration_range")
                      .count()
                      .orderBy("duration_range")
                      .collect())

    for row in duration_stats:
        pct = row["count"] / total_records * 100 if total_records > 0 else 0
        print(f"  {row['duration_range']}: {row['count']} 条 ({pct:.1f}%)")

    # ---- 5.3 付费玩家统计 ----
    paying_players = (df_labeled
                      .filter(F.col("is_paying_player_std") == 1)
                      .select("player_id")
                      .distinct()
                      .count())
    total_players = df_labeled.select("player_id").distinct().count()
    paying_rate = paying_players / total_players * 100 if total_players > 0 else 0
    total_recharge = (df_labeled
                      .agg(F.sum("recharge_amount_std"))
                      .collect()[0][0])

    print(f"\n[统计] 付费统计:")
    print(f"  付费玩家数: {paying_players} / {total_players} ({paying_rate:.1f}%)")
    print(f"  总充值金额: {total_recharge:.2f} 元")
    if paying_players > 0:
        print(f"  ARPPU(付费用户均价): {total_recharge/paying_players:.2f} 元")

    # ---- 5.4 各风险标签人数 ----
    print("\n[统计] 风险标签分布 (按去重玩家):")
    risk_player_stats = (df_labeled
                         .groupBy("risk_label")
                         .agg(F.countDistinct("player_id").alias("player_count"))
                         .orderBy("risk_label")
                         .collect())

    label_names = {
        0: "正常玩家",
        1: "一级预警(单日>4小时)",
        2: "二级违规(夜间登录)",
        3: "重度沉迷(时长+夜间)"
    }
    for row in risk_player_stats:
        name = label_names.get(row["risk_label"], f"未知")
        pct = row["player_count"] / total_players * 100 if total_players > 0 else 0
        print(f"  标签{row['risk_label']} {name}: {row['player_count']} 人 ({pct:.1f}%)")

    # ---- 5.5 未成年专项统计 ----
    print("\n[统计] 未成年玩家专项:")
    minor_df = df_labeled.filter(F.col("is_minor_std") == 1)
    minor_count = minor_df.select("player_id").distinct().count()

    # 未成年平均在线时长
    minor_avg_duration = (minor_df
                          .agg(F.avg("online_duration_std"))
                          .collect()[0][0])
    minor_avg_min = minor_avg_duration / 60 if minor_avg_duration else 0

    # 未成年各风险等级分布
    minor_risk_dist = (minor_df
                       .groupBy("risk_label")
                       .agg(F.countDistinct("player_id").alias("cnt"))
                       .orderBy("risk_label")
                       .collect())

    print(f"  未成年玩家数: {minor_count} 人")
    print(f"  未成年平均在线时长: {minor_avg_min:.1f} 分钟")
    print(f"  未成年风险分布:")
    for row in minor_risk_dist:
        name = label_names.get(row["risk_label"], f"未知")
        pct = row["cnt"] / minor_count * 100 if minor_count > 0 else 0
        print(f"    {name}: {row['cnt']} 人 ({pct:.1f}%)")

    # ---- 5.6 写入统计结果表 (可选) ----
    try:
        write_statistics_to_hive(df_labeled, dt, spark, dau, paying_players,
                                 total_players, total_recharge, minor_count)
        print("[统计] 统计结果已写入Hive统计表。")
    except Exception as e:
        print(f"[警告] 写入统计表失败 (可能表未创建): {e}")

    print("\n[统计] 日统计指标计算完成。")

    return df_labeled


def write_statistics_to_hive(df_labeled, dt, spark, dau, paying_players,
                              total_players, total_recharge, minor_count):
    """
    将统计结果写入Hive统计表 (ADS层聚合表)，使用Spark SQL确保类型正确。
    """
    total_rec = df_labeled.count()

    rl0 = df_labeled.filter(F.col("risk_label") == 0) \
                    .select("player_id").distinct().count()
    rl1 = df_labeled.filter(F.col("risk_label") == 1) \
                    .select("player_id").distinct().count()
    rl2 = df_labeled.filter(F.col("risk_label") == 2) \
                    .select("player_id").distinct().count()
    rl3 = df_labeled.filter(F.col("risk_label") == 3) \
                    .select("player_id").distinct().count()

    avg_min = round(df_labeled.agg(F.avg("online_duration_std"))
                    .collect()[0][0] / 60.0, 1) if total_rec > 0 else 0.0
    trec = float(total_recharge) if total_recharge else 0.0
    etl_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 使用 Spark SQL 精确控制类型，避免 DataFrame schema 推断问题
    insert_sql = f"""
        INSERT INTO game_anti_addiction.{STATS_TABLE} PARTITION (dt='{dt}')
        SELECT
            {dau}L           AS dau,
            {total_rec}L     AS total_records,
            {paying_players}L AS paying_players,
            {total_players}L AS total_players,
            {trec}           AS total_recharge,
            {minor_count}L   AS minor_players,
            {rl0}L           AS risk_label_0,
            {rl1}L           AS risk_label_1,
            {rl2}L           AS risk_label_2,
            {rl3}L           AS risk_label_3,
            {avg_min}        AS avg_online_min,
            '{etl_ts}'       AS etl_time
    """
    spark.sql(insert_sql)


# ============================================================================
# 模块六: 结果写入Hive DWD层
# ============================================================================

def write_to_dwd(df_labeled: DataFrame, dt: str, dwd_db: str, spark: SparkSession):
    """
    将清洗标注后的数据写入Hive DWD层明细表。

    写入策略:
      - 使用 INSERT OVERWRITE 覆盖目标分区 (幂等性: 可重复执行)
      - 字段映射: 中间列名 → DWD表正式列名
      - 动态分区写入

    参数:
        df_labeled: 带风险标签的标准DataFrame
        dt: 目标分区日期
        dwd_db: DWD层数据库名
    """
    dwd_full_name = f"{dwd_db}.{DWD_TABLE}"
    print(f"\n[写入] 开始写入DWD表: {dwd_full_name}, 分区: dt={dt}")

    # ---- 字段映射: 中间列名 → DWD表最终列名 ----
    # 确保字段名和类型与DWD表结构完全匹配
    df_dwd = (df_labeled
              .select(
                  F.col("player_id").alias("player_id"),
                  F.col("account_type_std").alias("account_type"),
                  F.col("game_id_std").alias("game_id"),
                  F.col("login_time_std").alias("login_time"),
                  F.col("logout_time_std").alias("logout_time"),
                  F.col("login_date_std").alias("login_date"),
                  F.col("login_hour_std").alias("login_hour"),
                  F.col("login_dayofweek_std").alias("login_dayofweek"),
                  F.col("online_duration_std").alias("online_duration"),
                  F.col("online_duration_min_std").alias("online_duration_min"),
                  F.col("match_count_std").alias("match_count"),
                  F.col("recharge_amount_std").alias("recharge_amount"),
                  F.col("item_consumption_std").alias("item_consumption"),
                  F.col("login_ip_std").alias("login_ip"),
                  F.col("login_period_std").alias("login_period"),
                  F.col("is_night_login_std").alias("is_night_login"),
                  F.col("game_region_std").alias("game_region"),
                  F.col("device_type_std").alias("device_type"),
                  F.col("is_minor_std").alias("is_minor"),
                  F.when(F.col("online_duration_std") > 14400, 1)
                   .otherwise(0).alias("is_heavy_gamer"),
                  F.col("is_paying_player_std").alias("is_paying_player"),
                  F.col("etl_time_std").alias("etl_time"),
                  F.col("source_file_std").alias("source_file"),
                  F.col("risk_label").alias("risk_label"),  # 防沉迷风险标签
                  F.lit(dt).alias("dt")  # 动态分区字段
              ))

    # 控制输出分区数 (避免大量小文件)
    num_partitions = max(1, df_dwd.rdd.getNumPartitions() // 2)
    df_dwd = df_dwd.coalesce(num_partitions)

    # 写入前检查: 如果DWD表不存在risk_label字段，则尝试添加
    # (首次运行时需要)
    try:
        # 使用 INSERT OVERWRITE 写入指定分区
        df_dwd.write \
            .mode("overwrite") \
            .format("hive") \
            .insertInto(dwd_full_name, overwrite=True)

        # 验证写入结果
        written_count = (spark.table(dwd_full_name)
                         .filter(F.col("dt") == dt)
                         .count())
        print(f"[写入] DWD分区 dt={dt} 写入完成! 记录数: {written_count}")

    except Exception as e:
        print(f"[错误] DWD表写入失败: {e}")
        print("[提示] 如果DWD表缺少risk_label列，请先执行:")
        print(f"  ALTER TABLE {dwd_full_name} ADD COLUMNS (risk_label INT "
              "COMMENT '防沉迷风险标签: 0正常/1一级预警/2二级违规/3重度沉迷');")
        raise


# ============================================================================
# 主函数
# ============================================================================

def main():
    """
    主控制流:
      1. 解析命令行参数
      2. 初始化SparkSession
      3. 读取ODS → 清洗 → 标准化 → 风险标注 → 统计 → 写入DWD
    """
    # ---- 参数解析 ----
    if len(sys.argv) < 2:
        print("=" * 70)
        print("  用法: spark-submit etl_anti_addiction.py <日期> [ODS库] [DWD库]")
        print("  示例: spark-submit --master yarn --deploy-mode client \\")
        print("           etl_anti_addiction.py 20260610")
        print("  示例: spark-submit --master yarn --deploy-mode client \\")
        print("           etl_anti_addiction.py 20260610 game_anti_addiction game_anti_addiction")
        print("=" * 70)
        sys.exit(1)

    etl_date = sys.argv[1]
    ods_db = sys.argv[2] if len(sys.argv) >= 3 else "game_anti_addiction"
    dwd_db = sys.argv[3] if len(sys.argv) >= 4 else "game_anti_addiction"

    # 验证日期格式
    try:
        datetime.strptime(etl_date, "%Y%m%d")
    except ValueError:
        print(f"[错误] 日期格式无效: {etl_date}, 必须为 yyyyMMdd 格式 (如 20260610)")
        sys.exit(1)

    print("=" * 70)
    print(f"  游戏平台玩家行为分析与防沉迷系统 - PySpark ETL")
    print(f"  处理日期: {etl_date}")
    print(f"  ODS数据库: {ods_db}")
    print(f"  DWD数据库: {dwd_db}")
    print("=" * 70)

    # ---- 初始化Spark ----
    spark = create_spark_session(f"GameAntiAddiction_ETL_{etl_date}")
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Step 1: 读取ODS原始数据
        df_raw = read_ods_data(spark, etl_date, ods_db)

        if df_raw.count() == 0:
            print(f"[警告] ODS表 {ods_db}.{ODS_TABLE} 分区 dt={etl_date} 无数据，ETL终止。")
            print("[提示] 请确认: 1) 日志生成脚本已运行 2) Flume已将数据写入HDFS 3) ODS分区已添加")
            return

        # Step 2: 数据清洗
        df_clean = clean_data(df_raw)

        # Step 3: 时间标准化 + 格式统一
        df_std = standardize_data(df_clean)
        # 缓存标准化数据 (后续多次使用)
        df_std.cache()

        # Step 4: 沉迷风险标签生成
        df_labeled = add_risk_labels(df_std)

        # Step 5: 统计指标计算
        df_labeled = compute_statistics(df_labeled, etl_date, spark)

        # Step 6: 写入DWD层
        write_to_dwd(df_labeled, etl_date, dwd_db, spark)

        # ---- 释放缓存 ----
        df_std.unpersist()

        print("\n" + "=" * 70)
        print(f"  ETL任务执行完成! 日期: {etl_date}")
        print("=" * 70)

    except Exception as e:
        print(f"\n[严重错误] ETL任务失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        spark.stop()
        print("[信息] SparkSession已关闭。")


# ============================================================================
# 程序入口
# ============================================================================
if __name__ == "__main__":
    main()
