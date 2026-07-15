-- ============================================================================
-- 游戏平台玩家行为分析与防沉迷系统
-- Hive 3.1.2 数据仓库建表语句
-- 数据架构: ODS层(操作数据层) → DWD层(明细数据层)
-- ============================================================================
-- 执行方式: beeline -u "jdbc:hive2://hiveserver2:10000" -f hive_ddl.sql
--           或在 hive> 客户端中逐段执行
-- ============================================================================


-- ============================================================================
-- 第一部分: 创建数据库
-- ============================================================================
CREATE DATABASE IF NOT EXISTS game_anti_addiction
COMMENT '游戏平台玩家行为分析与防沉迷系统数据仓库'
LOCATION '/user/hive/warehouse/game_anti_addiction.db';

USE game_anti_addiction;


-- ============================================================================
-- 第二部分: ODS层 - 操作数据存储层
-- ============================================================================
-- 设计原则:
--   1. 外部表(EXTERNAL): 删除表不删除HDFS原始数据，安全性高
--   2. 全STRING存储: 保留原始日志格式，不做类型转换
--   3. 按dt分区: 以日期为分区键，目录结构与HDFS路径 dt=%Y%m%d 对齐
--   4. 文本格式: 与Flume落地格式一致，减少ETL开销
--   5. 字段分隔符: \| (管道符)，与日志生成脚本和Flume保持一致
-- ============================================================================

DROP TABLE IF EXISTS ods_game_player_behavior;

CREATE EXTERNAL TABLE IF NOT EXISTS ods_game_player_behavior (
    player_id           STRING  COMMENT '玩家ID，如 PLAYER_00000001',
    account_type        STRING  COMMENT '账号类型: minor-未成年, adult-成年',
    game_id             STRING  COMMENT '游戏ID，如 GAME_001',
    login_time          STRING  COMMENT '登录时间，格式 yyyy-MM-dd HH:mm:ss',
    logout_time         STRING  COMMENT '下线时间，格式 yyyy-MM-dd HH:mm:ss',
    online_duration     STRING  COMMENT '在线时长(秒)，范围 0~21600',
    match_count         STRING  COMMENT '对局次数',
    recharge_amount     STRING  COMMENT '充值金额(元)，保留两位小数',
    item_consumption    STRING  COMMENT '道具消费数量',
    login_ip            STRING  COMMENT '登录IP地址',
    login_period        STRING  COMMENT '登录时段: 白天(06-18)/傍晚(18-24)/夜间(00-06)',
    game_region         STRING  COMMENT '游戏区域，如 华东一区、华南一区',
    device_type         STRING  COMMENT '设备类型: android/ios/pc'
)
COMMENT 'ODS层 - 游戏玩家行为日志原始数据(外部表)'
PARTITIONED BY (
    dt                  STRING  COMMENT '分区字段: 日志日期，格式 yyyyMMdd，如 20260610'
)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '|'          -- 字段分隔符: 竖线
    LINES TERMINATED BY '\n'          -- 行分隔符: 换行
STORED AS TEXTFILE                    -- 文本格式存储
LOCATION '/user/flume/game_logs'      -- HDFS原始日志目录(Flume HDFS Sink落地路径)
TBLPROPERTIES (
    'creator'           = 'game_anti_addiction_system',
    'created_date'      = '2026-06-10',
    'data_source'       = 'Flume_HDFS_Sink',
    'log_format'        = 'pipe_delimited',
    'skip.header.line.count' = '0',   -- 无表头，直接是数据行
    'serialization.null.format' = ''  -- 空字段保持为空字符串
);


-- ============================================================================
-- 第三部分: DWD层 - 数据明细层
-- ============================================================================
-- 设计原则:
--   1. 内部表(MANAGED): 数据由Hive管理，ETL后归入仓库目录
--   2. 字段类型规范: 时间→TIMESTAMP，数值→BIGINT/DOUBLE，类别→STRING
--   3. 新增派生字段: 方便后续Spark/Flink分析直接使用，减少重复计算
--   4. 按dt分区: 与ODS层分区对齐，逐日ETL
--   5. ORC格式 + Snappy压缩: 列式存储，查询效率高、占用空间小
-- ============================================================================

DROP TABLE IF EXISTS dwd_game_player_behavior;

CREATE TABLE IF NOT EXISTS dwd_game_player_behavior (
    -- 主键与标识字段
    player_id           STRING      COMMENT '玩家ID',
    account_type        STRING      COMMENT '账号类型: minor/adult',
    game_id             STRING      COMMENT '游戏ID',

    -- 时间字段(转换为TIMESTAMP类型，便于时间计算)
    login_time          TIMESTAMP   COMMENT '登录时间',
    logout_time         TIMESTAMP   COMMENT '下线时间',
    login_date          DATE        COMMENT '登录日期(派生字段)',
    login_hour          INT         COMMENT '登录小时 0-23(派生字段)',
    login_dayofweek     INT         COMMENT '登录星期几 1=周一 7=周日(派生字段)',

    -- 行为指标字段
    online_duration     BIGINT      COMMENT '在线时长(秒)',
    online_duration_min DOUBLE      COMMENT '在线时长(分钟，派生字段，保留1位小数)',
    match_count         INT         COMMENT '对局次数',
    recharge_amount     DOUBLE      COMMENT '充值金额(元)',
    item_consumption    BIGINT      COMMENT '道具消费数量',

    -- 属性字段
    login_ip            STRING      COMMENT '登录IP',
    login_period        STRING      COMMENT '登录时段: 白天/傍晚/夜间',
    is_night_login      INT         COMMENT '是否夜间登录: 1=是 0=否(派生字段)',
    game_region         STRING      COMMENT '游戏区域',
    device_type         STRING      COMMENT '设备类型: android/ios/pc',

    -- 防沉迷标识字段
    is_minor            INT         COMMENT '是否未成年: 1=是 0=否(派生字段)',
    is_heavy_gamer      INT         COMMENT '是否重度玩家(在线>4小时): 1=是 0=否(派生字段)',
    is_paying_player    INT         COMMENT '是否付费玩家: 1=是 0=否(派生字段)',

    -- 数据溯源字段
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间',
    source_file         STRING      COMMENT '数据来源HDFS文件路径(输入文件名)'
)
COMMENT 'DWD层 - 游戏玩家行为明细清洗表(分区ORC表)'
PARTITIONED BY (
    dt                  STRING      COMMENT '分区字段: 数据日期，格式 yyyyMMdd'
)
CLUSTERED BY (player_id) SORTED BY (login_time) INTO 32 BUCKETS   -- 按玩家ID分桶，优化JOIN查询
STORED AS ORC                                                      -- ORC列式存储
LOCATION '/user/hive/warehouse/game_anti_addiction.db/dwd_game_player_behavior'
TBLPROPERTIES (
    'creator'                   = 'game_anti_addiction_system',
    'created_date'              = '2026-06-10',
    'orc.compress'              = 'SNAPPY',          -- Snappy压缩，兼顾速度与体积
    'orc.create.index'          = 'true',             -- 创建ORC索引
    'orc.bloom.filter.columns'  = 'player_id,game_id', -- 为高频过滤字段创建布隆过滤器
    'orc.stripe.size'           = '268435456'         -- 256MB Stripe大小，与HDFS块对齐
);


-- ============================================================================
-- 第四部分: 数据加载 ETL 语句
-- ============================================================================

-- --------------------------------------------------------------------------
-- 4.1 ODS层数据加载 - 关联HDFS分区目录
-- --------------------------------------------------------------------------
-- 原理说明:
--   Flume HDFS Sink按 /user/flume/game_logs/dt=YYYYMMDD/ 目录结构写入文件
--   Hive外部表通过 MSCK REPAIR TABLE 或 ALTER TABLE ADD PARTITION 关联分区
--   此处使用动态分区添加，将HDFS目录映射为Hive分区

-- 方式1: 自动修复所有分区（推荐首次使用或每天执行一次）
-- MSCK REPAIR TABLE ods_game_player_behavior;

-- 方式2: 手动添加指定日期的分区（精确控制，推荐日常调度使用）
-- 示例: 加载2026年6月10日的数据
ALTER TABLE ods_game_player_behavior
ADD IF NOT EXISTS PARTITION (dt='20260610')
LOCATION '/user/flume/game_logs/dt=20260610';

-- 示例: 加载2026年6月11日的数据
-- ALTER TABLE ods_game_player_behavior
-- ADD IF NOT EXISTS PARTITION (dt='20260611')
-- LOCATION '/user/flume/game_logs/dt=20260611';


-- --------------------------------------------------------------------------
-- 4.2 DWD层数据加载 - ODS清洗转换到DWD
-- --------------------------------------------------------------------------
-- ETL逻辑:
--   1. 字段类型转换: STRING → TIMESTAMP/BIGINT/DOUBLE/INT
--   2. 派生字段计算: login_date, login_hour, is_minor, is_night_login 等
--   3. 数据质量过滤: 剔除明显异常数据（如在线时长为负数）
--   4. 插入前清空目标分区(幂等性: 可重复执行)

-- 设置Hive执行参数（优化ETL性能）
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions = 1000;
SET hive.exec.max.dynamic.partitions.pernode = 500;
SET mapreduce.map.memory.mb = 2048;
SET mapreduce.reduce.memory.mb = 4096;
SET hive.exec.compress.output = true;
SET mapreduce.output.fileoutputformat.compress = true;
SET mapreduce.output.fileoutputformat.compress.codec = org.apache.hadoop.io.compress.SnappyCodec;

-- 将Hive执行引擎设置为Spark（如果配置了Spark on Hive，注释掉则用默认MR）
-- SET hive.execution.engine = spark;

-- 覆盖写入指定分区（生产调度中替换 ${etl_date} 为实际日期）
INSERT OVERWRITE TABLE dwd_game_player_behavior
PARTITION (dt)
SELECT
    -- === 主键与标识字段: 直接映射 ===
    player_id,
    account_type,
    game_id,

    -- === 时间字段: STRING → TIMESTAMP 类型转换 ===
    -- 登录时间转换，格式: yyyy-MM-dd HH:mm:ss
    CAST(login_time AS TIMESTAMP)                           AS login_time,
    -- 下线时间转换
    CAST(logout_time AS TIMESTAMP)                          AS logout_time,
    -- 派生: 提取登录日期
    CAST(SUBSTR(login_time, 1, 10) AS DATE)                 AS login_date,
    -- 派生: 提取登录小时(0-23)
    CAST(SUBSTR(login_time, 12, 2) AS INT)                  AS login_hour,
    -- 派生: 计算星期几(1=周一 ~ 7=周日)，用于分析周末/工作日行为差异
    -- DAYOFWEEK在Hive中: 1=周日, 转换到 ISO标准 1=周一
    CASE
        WHEN DAYOFWEEK(CAST(SUBSTR(login_time, 1, 10) AS DATE)) = 1 THEN 7
        ELSE DAYOFWEEK(CAST(SUBSTR(login_time, 1, 10) AS DATE)) - 1
    END                                                     AS login_dayofweek,

    -- === 行为指标: STRING → 数值 类型转换 ===
    CAST(online_duration AS BIGINT)                         AS online_duration,
    -- 派生: 在线分钟数，保留1位小数
    ROUND(CAST(online_duration AS DOUBLE) / 60.0, 1)        AS online_duration_min,
    CAST(match_count AS INT)                                AS match_count,
    CAST(recharge_amount AS DOUBLE)                         AS recharge_amount,
    CAST(item_consumption AS BIGINT)                        AS item_consumption,

    -- === 属性字段: 直接映射 ===
    login_ip,
    login_period,
    -- 派生: 是否夜间登录标识（夜间=00:00~05:59）
    IF(login_period = '夜间', 1, 0)                         AS is_night_login,
    game_region,
    device_type,

    -- === 防沉迷标识: 派生计算 ===
    -- 是否未成年
    IF(account_type = 'minor', 1, 0)                        AS is_minor,
    -- 是否重度玩家: 在线时长超过4小时(14400秒)
    IF(CAST(online_duration AS BIGINT) > 14400, 1, 0)       AS is_heavy_gamer,
    -- 是否付费玩家: 充值金额大于0
    IF(CAST(recharge_amount AS DOUBLE) > 0, 1, 0)           AS is_paying_player,

    -- === 数据溯源 ===
    CURRENT_TIMESTAMP()                                     AS etl_time,
    -- 通过INPUT__FILE__NAME获取HDFS源文件路径，方便数据回溯
    INPUT__FILE__NAME                                       AS source_file,

    -- === 分区字段: 取登录时间的日期部分 ===
    REGEXP_REPLACE(SUBSTR(login_time, 1, 10), '-', '')      AS dt

FROM ods_game_player_behavior
WHERE dt = '${etl_date}'                                    -- 按分区过滤，只读目标日期的ODS数据
  -- 数据质量过滤: 剔除明显异常记录
  AND player_id IS NOT NULL
  AND player_id != ''
  AND login_time IS NOT NULL
  AND logout_time IS NOT NULL
  AND CAST(online_duration AS BIGINT) >= 0
  AND CAST(online_duration AS BIGINT) <= 21600             -- 最大6小时
  AND login_time < logout_time;                             -- 登录时间必须早于下线时间


-- --------------------------------------------------------------------------
-- 4.3 验证查询 - 检查数据加载结果
-- --------------------------------------------------------------------------

-- 验证ODS层分区与数据量
SELECT
    'ODS'                           AS layer,
    dt,
    COUNT(*)                        AS record_count,
    COUNT(DISTINCT player_id)       AS unique_players,
    COUNT(DISTINCT game_id)         AS unique_games
FROM ods_game_player_behavior
WHERE dt = '20260610'
GROUP BY dt;

-- 验证DWD层数据质量
SELECT
    'DWD'                           AS layer,
    dt,
    COUNT(*)                        AS record_count,
    -- 未成年占比
    ROUND(SUM(is_minor) / COUNT(*) * 100, 1)    AS minor_pct,
    -- 夜间登录占比
    ROUND(SUM(is_night_login) / COUNT(*) * 100, 1) AS night_pct,
    -- 未成年夜间登录占比（防沉迷核心指标）
    ROUND(SUM(CASE WHEN is_minor=1 AND is_night_login=1 THEN 1 ELSE 0 END)
          / NULLIF(SUM(is_minor), 0) * 100, 1)   AS minor_night_pct,
    -- 重度玩家占比
    ROUND(SUM(is_heavy_gamer) / COUNT(*) * 100, 1) AS heavy_pct,
    -- 平均在线时长(分钟)
    ROUND(AVG(online_duration_min), 1)              AS avg_online_min,
    -- 付费率
    ROUND(SUM(is_paying_player) / COUNT(*) * 100, 1) AS paying_pct,
    -- 总充值金额
    ROUND(SUM(recharge_amount), 2)                  AS total_recharge
FROM dwd_game_player_behavior
WHERE dt = '20260610'
GROUP BY dt;

-- 验证DWD层分区数据概览（按维度切片）
SELECT
    dt,
    account_type,
    login_period,
    COUNT(*)                        AS cnt,
    ROUND(AVG(online_duration_min), 1) AS avg_online_min,
    ROUND(AVG(recharge_amount), 2)     AS avg_recharge,
    ROUND(AVG(match_count), 1)         AS avg_matches
FROM dwd_game_player_behavior
WHERE dt = '20260610'
GROUP BY dt, account_type, login_period
ORDER BY account_type, login_period;


-- ============================================================================
-- 第五部分: 调度模板 - 每日增量ETL脚本
-- ============================================================================
-- 生产环境中，以下逻辑应封装为Shell脚本，由Azkaban/Airflow/DolphinScheduler调度
-- 变量 ${etl_date} 由调度系统传入，格式 yyyyMMdd，如 20260610

-- 每日ETL流程:
-- Step 1: 添加ODS分区（指向当日HDFS目录）
--   ALTER TABLE ods_game_player_behavior
--   ADD IF NOT EXISTS PARTITION (dt='${etl_date}');

-- Step 2: 执行DWD清洗转换（见上方 INSERT OVERWRITE 语句）

-- Step 3: 可选 - 清理过期ODS分区（保留最近30天）
--   ALTER TABLE ods_game_player_behavior
--   DROP IF EXISTS PARTITION (dt < '${etl_date_30_days_ago}');


-- ============================================================================
-- 第六部分: DWD表字段扩展 - 添加防沉迷风险标签列
-- ============================================================================
-- 说明: 如果DWD表首次创建时不包含risk_label字段，执行此ALTER语句添加
-- 此语句幂等：字段已存在时跳过

ALTER TABLE dwd_game_player_behavior
ADD COLUMNS (
    risk_label          INT     COMMENT '防沉迷风险标签: 0-正常/1-一级预警(单日>4h)/2-二级违规(夜间登录)/3-重度沉迷(时长+夜间)'
);


-- ============================================================================
-- 第七部分: ADS层 - 每日防沉迷统计聚合表
-- ============================================================================
-- 设计原则:
--   1. 存储每日统计指标，为BI看板/报表提供聚合数据
--   2. 按dt分区，每日一条汇总记录
--   3. ORC存储 + Snappy压缩

DROP TABLE IF EXISTS ads_anti_addiction_daily_stats;

CREATE TABLE IF NOT EXISTS ads_anti_addiction_daily_stats (
    dau                 BIGINT      COMMENT '日活跃玩家数(DAU)',
    total_records       BIGINT      COMMENT '总行为记录数',
    paying_players      BIGINT      COMMENT '付费玩家数',
    total_players       BIGINT      COMMENT '总玩家数(去重)',
    total_recharge      DOUBLE      COMMENT '总充值金额(元)',
    minor_players       BIGINT      COMMENT '未成年玩家总数',
    risk_label_0        BIGINT      COMMENT '风险标签0: 正常玩家数',
    risk_label_1        BIGINT      COMMENT '风险标签1: 一级预警人数',
    risk_label_2        BIGINT      COMMENT '风险标签2: 二级违规人数',
    risk_label_3        BIGINT      COMMENT '风险标签3: 重度沉迷人数',
    avg_online_min      DOUBLE      COMMENT '平均在线时长(分钟)',
    etl_time            STRING      COMMENT 'ETL处理时间'
)
COMMENT 'ADS层 - 游戏防沉迷每日统计聚合表'
PARTITIONED BY (
    dt                  STRING      COMMENT '统计日期，格式 yyyyMMdd'
)
STORED AS ORC
LOCATION '/user/hive/warehouse/game_anti_addiction.db/ads_anti_addiction_daily_stats'
TBLPROPERTIES (
    'creator'       = 'game_anti_addiction_system',
    'orc.compress'  = 'SNAPPY'
);


-- ============================================================================
-- 完成提示
-- ============================================================================
-- 执行完毕后，建议运行以下命令检查表结构:
--   DESCRIBE FORMATTED ods_game_player_behavior;
--   DESCRIBE FORMATTED dwd_game_player_behavior;
--   DESCRIBE FORMATTED ads_anti_addiction_daily_stats;
--   SHOW PARTITIONS ods_game_player_behavior;
--   SHOW PARTITIONS dwd_game_player_behavior;
