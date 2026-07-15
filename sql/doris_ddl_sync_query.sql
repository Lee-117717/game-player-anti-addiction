-- ============================================================================
-- 游戏平台玩家行为分析与防沉迷系统
-- Apache Doris 2.1.x 建表、数据同步与多维分析SQL
-- ============================================================================
-- 架构说明:
--   Hive DWD (HDFS) ──→ Doris (OLAP引擎)
--   同步方式1: Hive External Catalog → INSERT INTO SELECT (推荐)
--   同步方式2: Broker Load → 批量导入HDFS文件
--   用途: 实时多维分析、BI看板、防沉迷监控大屏
-- ============================================================================


-- ============================================================================
-- 第一部分: Doris 建表语句 (DUPLICATE KEY 明细模型)
-- ============================================================================
-- 设计原则:
--   1. DUPLICATE KEY 模型: 保留所有明细行，不做预聚合，适合行为日志分析
--   2. 分区策略: 按 dt (日期) RANGE 分区，与Hive分区对齐，支持分区裁剪
--   3. 分桶策略: 按 player_id HASH 分桶，玩家维度查询可精确定位
--   4. 字段类型: 与Hive DWD层类型对齐，时间用DATETIME，类别用VARCHAR
--   5. 副本数: 2副本保证高可用
--   6. 压缩: LZ4 压缩，查询/存储平衡
-- ============================================================================

CREATE DATABASE IF NOT EXISTS game_anti_addiction;

USE game_anti_addiction;

-- 删除旧表（首次执行或重建时）
DROP TABLE IF EXISTS dwd_game_player_behavior;

CREATE TABLE IF NOT EXISTS dwd_game_player_behavior (
    -- ========================
    -- 分区字段
    -- ========================
    dt                  DATE            COMMENT '分区日期 (与Hive分区对齐)',

    -- ========================
    -- 主键与标识字段
    -- ========================
    player_id           VARCHAR(64)     COMMENT '玩家ID，如 PLAYER_00000001',
    account_type        VARCHAR(10)     COMMENT '账号类型: minor-未成年, adult-成年',
    game_id             VARCHAR(20)     COMMENT '游戏ID，如 GAME_001',

    -- ========================
    -- 时间字段
    -- ========================
    login_time          DATETIME        COMMENT '登录时间',
    logout_time         DATETIME        COMMENT '下线时间',
    login_date          DATE            COMMENT '登录日期(派生)',
    login_hour          TINYINT         COMMENT '登录小时 0~23(派生)',
    login_dayofweek     TINYINT         COMMENT '登录星期几 1=周一 7=周日(派生)',

    -- ========================
    -- 行为指标字段
    -- ========================
    online_duration     INT             COMMENT '在线时长(秒)',
    online_duration_min DECIMAL(10,1)   COMMENT '在线时长(分钟，保留1位小数)',
    match_count         SMALLINT        COMMENT '对局次数',
    recharge_amount     DECIMAL(12,2)   COMMENT '充值金额(元)',
    item_consumption    INT             COMMENT '道具消费数量',

    -- ========================
    -- 属性字段
    -- ========================
    login_ip            VARCHAR(20)     COMMENT '登录IP地址',
    login_period        VARCHAR(10)     COMMENT '登录时段: 白天/傍晚/夜间',
    is_night_login      TINYINT         COMMENT '是否夜间登录: 1=是 0=否(派生)',
    game_region         VARCHAR(20)     COMMENT '游戏区域，如 华东一区',
    device_type         VARCHAR(10)     COMMENT '设备类型: android/ios/pc',

    -- ========================
    -- 防沉迷标识字段
    -- ========================
    is_minor            TINYINT         COMMENT '是否未成年: 1=是 0=否(派生)',
    is_heavy_gamer      TINYINT         COMMENT '是否重度玩家(在线>4h): 1=是 0=否(派生)',
    is_paying_player    TINYINT         COMMENT '是否付费玩家: 1=是 0=否(派生)',
    risk_label          TINYINT         COMMENT '沉迷风险标签: 0-正常/1-一级预警/2-二级违规/3-重度沉迷',

    -- ========================
    -- 数据溯源字段
    -- ========================
    etl_time            DATETIME        COMMENT 'ETL处理时间',
    source_file         VARCHAR(256)    COMMENT '数据来源HDFS文件路径'
)
ENGINE = OLAP
DUPLICATE KEY (dt, player_id, login_time)       -- 明细模型，按分区+玩家+登录时间排序
PARTITION BY RANGE (dt) ()                  -- 动态创建分区，按日期范围
DISTRIBUTED BY HASH (player_id) BUCKETS 32  -- 按玩家ID哈希分32桶
PROPERTIES (
    -- 副本数: 2副本保证HA
    'replication_num'               = '2',
    -- 存储压缩算法: LZ4 (查询快) / ZSTD (压缩率高)
    'compression'                   = 'LZ4',
    -- 是否允许写入副本所在节点之外
    'disable_auto_compaction'       = 'false',
    -- 动态分区配置 (自动创建/删除分区，无需手动管理)
    'dynamic_partition.enable'      = 'true',
    'dynamic_partition.time_unit'   = 'DAY',
    'dynamic_partition.start'       = '-30',     -- 保留过去30天分区
    'dynamic_partition.end'         = '3',       -- 预创建未来3天分区
    'dynamic_partition.prefix'      = 'p',
    'dynamic_partition.buckets'     = '32',
    -- 存储策略
    'storage_policy'                = 'default',
    -- 冷热分层 (可选)
    'storage_cooldown_time'         = '2026-07-01 00:00:00'
);

-- 手动创建初始分区 (以2026年6月10日为例)
ALTER TABLE dwd_game_player_behavior
ADD PARTITION IF NOT EXISTS p20260610
VALUES [('20260610'), ('20260611'));


-- ============================================================================
-- 第二部分: Hive → Doris 数据同步方案
-- ============================================================================

-- --------------------------------------------------------------------------
-- 方案一: Hive External Catalog 直接查询导入 (推荐⭐⭐⭐⭐⭐)
-- --------------------------------------------------------------------------
-- 原理:
--   Doris 2.0+ 支持 Multi-Catalog，创建 Hive Catalog 后可直接查询 Hive 表
--   通过 INSERT INTO SELECT 将数据从 Hive 导入 Doris
-- 优点:
--   - 无需中间文件，直接Doris内完成
--   - 支持增量同步（按分区）
--   - 配置简单，一条SQL完成同步
-- 适用: Doris 2.0+, Hive 3.x

-- Step 1: 创建 Hive Catalog 连接
-- 注意: 需要将 hive-site.xml 放置在 FE/BE 的 conf 目录下，或通过 properties 指定
CREATE CATALOG IF NOT EXISTS hive_catalog
PROPERTIES (
    -- Catalog 类型
    'type'                          = 'hms',
    -- Hive Metastore URI (根据实际环境修改)
    'hive.metastore.uris'           = 'thrift://hive-metastore:9083',
    -- HDFS NameNode 地址 (根据实际环境修改)
    'hadoop.username'               = 'hadoop',
    -- HDFS 配置
    'dfs.nameservices'              = 'hdfs-cluster',
    'dfs.ha.namenodes.hdfs-cluster' = 'nn1,nn2',
    'dfs.namenode.rpc-address.hdfs-cluster.nn1' = 'namenode1:8020',
    'dfs.namenode.rpc-address.hdfs-cluster.nn2' = 'namenode2:8020',
    'dfs.client.failover.proxy.provider.hdfs-cluster'
        = 'org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider',
    -- Kerberos 认证 (如未开启则注释掉)
    -- 'hadoop.security.authentication'  = 'kerberos',
    -- 'hadoop.kerberos.principal'       = 'hadoop/_HOST@REALM',
    -- 'hadoop.kerberos.keytab'          = '/path/to/hadoop.keytab',
    -- 兼容性设置
    'file.meta.cache.ttl-second'    = '300'
);

-- Step 2: 验证 Catalog 连接
-- 列出 Hive 数据库
SHOW DATABASES FROM hive_catalog;

-- 查看 Hive DWD 表结构
DESCRIBE hive_catalog.game_anti_addiction.dwd_game_player_behavior;

-- 预览 Hive 数据
SELECT COUNT(*) FROM hive_catalog.game_anti_addiction.dwd_game_player_behavior
WHERE dt = '20260610';

-- Step 3: 全量同步 Hive → Doris (按分区批量导入)
-- 方式A: 单分区同步 (日常增量)
INSERT INTO dwd_game_player_behavior
SELECT
    player_id, account_type, game_id,
    login_time, logout_time, login_date,
    CAST(login_hour AS TINYINT)         AS login_hour,
    CAST(login_dayofweek AS TINYINT)    AS login_dayofweek,
    CAST(online_duration AS INT)        AS online_duration,
    CAST(online_duration_min AS DECIMAL(10,1)) AS online_duration_min,
    CAST(match_count AS SMALLINT)       AS match_count,
    CAST(recharge_amount AS DECIMAL(12,2)) AS recharge_amount,
    CAST(item_consumption AS INT)       AS item_consumption,
    login_ip, login_period,
    CAST(is_night_login AS TINYINT)     AS is_night_login,
    game_region, device_type,
    CAST(is_minor AS TINYINT)           AS is_minor,
    CAST(is_heavy_gamer AS TINYINT)     AS is_heavy_gamer,
    CAST(is_paying_player AS TINYINT)   AS is_paying_player,
    CAST(risk_label AS TINYINT)         AS risk_label,
    etl_time, source_file
FROM hive_catalog.game_anti_addiction.dwd_game_player_behavior
WHERE dt = '20260610';

-- 方式B: 多分区批量同步 (首次全量/补数据)
-- 循环调用每个分区，建议在调度脚本中实现
INSERT INTO dwd_game_player_behavior
SELECT
    player_id, account_type, game_id,
    login_time, logout_time, login_date,
    CAST(login_hour AS TINYINT)         AS login_hour,
    CAST(login_dayofweek AS TINYINT)    AS login_dayofweek,
    CAST(online_duration AS INT)        AS online_duration,
    CAST(online_duration_min AS DECIMAL(10,1)) AS online_duration_min,
    CAST(match_count AS SMALLINT)       AS match_count,
    CAST(recharge_amount AS DECIMAL(12,2)) AS recharge_amount,
    CAST(item_consumption AS INT)       AS item_consumption,
    login_ip, login_period,
    CAST(is_night_login AS TINYINT)     AS is_night_login,
    game_region, device_type,
    CAST(is_minor AS TINYINT)           AS is_minor,
    CAST(is_heavy_gamer AS TINYINT)     AS is_heavy_gamer,
    CAST(is_paying_player AS TINYINT)   AS is_paying_player,
    CAST(risk_label AS TINYINT)         AS risk_label,
    etl_time, source_file
FROM hive_catalog.game_anti_addiction.dwd_game_player_behavior
WHERE dt IN ('20260601','20260602','20260603','20260604','20260605',
             '20260606','20260607','20260608','20260609','20260610');


-- --------------------------------------------------------------------------
-- 方案二: Broker Load 批量导入 (备选)
-- --------------------------------------------------------------------------
-- 原理:
--   Doris Broker进程读取HDFS上的Hive DWD目录文件，直接加载到Doris表
-- 优点:
--   - 不依赖Hive Metastore，直接从HDFS读取
--   - 适合大规模历史数据首次全量导入
--   - 支持ORC/Parquet列式文件直接解析
-- 适用: Hive和Doris网络隔离、超大规模首次导入

-- Step 1: 检查 Broker 是否正常运行
SHOW PROC '/brokers';

-- Step 2: 执行 Broker Load (以2026-06-10分区为例)
-- Hive DWD 数据存储在 HDFS 路径:
--   /user/hive/warehouse/game_anti_addiction.db/dwd_game_player_behavior/dt=20260610/
LOAD LABEL game_db.label_20260610 (
    DATA INFILE (
        "hdfs://namenode:8020/user/hive/warehouse/game_anti_addiction.db/"
        "dwd_game_player_behavior/dt=20260610/*"
    )
    INTO TABLE dwd_game_player_behavior
    FORMAT AS "ORC"                            -- Hive DWD 为 ORC 格式
    (
        player_id, account_type, game_id,
        login_time, logout_time, login_date,
        login_hour, login_dayofweek,
        online_duration, online_duration_min,
        match_count, recharge_amount, item_consumption,
        login_ip, login_period, is_night_login,
        game_region, device_type,
        is_minor, is_heavy_gamer, is_paying_player,
        risk_label, etl_time, source_file
    )
    -- 列映射: Hive ORC列 → Doris表列 (按位置一一对应)
    SET (
        dt = "20260610"                         -- 分区字段手动指定
    )
)
WITH BROKER "hdfs_broker" (
    "dfs.nameservices" = "hdfs-cluster",
    "dfs.ha.namenodes.hdfs-cluster" = "nn1,nn2",
    "dfs.namenode.rpc-address.hdfs-cluster.nn1" = "namenode1:8020",
    "dfs.namenode.rpc-address.hdfs-cluster.nn2" = "namenode2:8020",
    "dfs.client.failover.proxy.provider.hdfs-cluster"
        = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
)
PROPERTIES (
    "timeout"           = "3600",               -- 超时时间(秒)
    "max_filter_ratio"  = "0.01",               -- 容错率1%，超过则任务失败
    "strict_mode"       = "false"
);

-- 查看 Broker Load 任务状态
SHOW LOAD WHERE LABEL = 'label_20260610' ORDER BY CreateTime DESC LIMIT 10;


-- --------------------------------------------------------------------------
-- 方案三: 增量同步调度脚本 (Shell模板)
-- --------------------------------------------------------------------------
-- 说明: 此脚本由 DolphinScheduler/Airflow 每日调度，实现增量同步
-- 变量: ${etl_date} 由调度系统传入，格式 yyyyMMdd

-- 每日增量同步SQL (Doris中执行):
-- 步骤1: 创建目标分区 (如动态分区未自动创建)
ALTER TABLE dwd_game_player_behavior
ADD PARTITION IF NOT EXISTS p${etl_date}
VALUES [('${etl_date}'), (DATE_FORMAT(DATE_ADD(STR_TO_DATE('${etl_date}','%Y%m%d'), INTERVAL 1 DAY), '%Y%m%d'))];

-- 步骤2: 从Hive Catalog同步当日分区数据
INSERT INTO dwd_game_player_behavior
SELECT
    player_id, account_type, game_id,
    login_time, logout_time, login_date,
    CAST(login_hour AS TINYINT),
    CAST(login_dayofweek AS TINYINT),
    CAST(online_duration AS INT),
    CAST(online_duration_min AS DECIMAL(10,1)),
    CAST(match_count AS SMALLINT),
    CAST(recharge_amount AS DECIMAL(12,2)),
    CAST(item_consumption AS INT),
    login_ip, login_period,
    CAST(is_night_login AS TINYINT),
    game_region, device_type,
    CAST(is_minor AS TINYINT),
    CAST(is_heavy_gamer AS TINYINT),
    CAST(is_paying_player AS TINYINT),
    CAST(risk_label AS TINYINT),
    etl_time, source_file
FROM hive_catalog.game_anti_addiction.dwd_game_player_behavior
WHERE dt = '${etl_date}';

-- 步骤3: 验证同步结果
SELECT
    '${etl_date}'                              AS sync_date,
    COUNT(*)                                   AS synced_rows,
    COUNT(DISTINCT player_id)                  AS synced_players,
    SUM(is_minor)                              AS minor_count,
    SUM(CASE WHEN risk_label > 0 THEN 1 ELSE 0 END) AS at_risk_count
FROM dwd_game_player_behavior
WHERE dt = '${etl_date}';


-- ============================================================================
-- 第三部分: 防沉迷多维分析查询SQL
-- ============================================================================

-- --------------------------------------------------------------------------
-- 3.1 按日期统计各沉迷风险标签人数
-- --------------------------------------------------------------------------
-- 用途: 防沉迷日度监控看板，展示各风险等级趋势
-- 可视化: 堆叠柱状图 / 趋势折线图

SELECT
    dt                                              AS `日期`,
    COUNT(DISTINCT player_id)                       AS `日活玩家数`,
    -- 各风险标签去重人数
    COUNT(DISTINCT CASE WHEN risk_label = 0
        THEN player_id END)                         AS `正常玩家`,
    COUNT(DISTINCT CASE WHEN risk_label = 1
        THEN player_id END)                         AS `一级预警(>4h)`,
    COUNT(DISTINCT CASE WHEN risk_label = 2
        THEN player_id END)                         AS `二级违规(夜间)`,
    COUNT(DISTINCT CASE WHEN risk_label = 3
        THEN player_id END)                         AS `重度沉迷`,
    -- 有风险行为玩家总数 (标签1+2+3)
    COUNT(DISTINCT CASE WHEN risk_label > 0
        THEN player_id END)                         AS `风险玩家总数`,
    -- 风险玩家占比
    ROUND(
        COUNT(DISTINCT CASE WHEN risk_label > 0 THEN player_id END)
        / COUNT(DISTINCT player_id) * 100, 2
    )                                               AS `风险玩家占比(%)`,
    -- 重度沉迷占比
    ROUND(
        COUNT(DISTINCT CASE WHEN risk_label = 3 THEN player_id END)
        / NULLIF(COUNT(DISTINCT player_id), 0) * 100, 2
    )                                               AS `重度沉迷占比(%)`
FROM dwd_game_player_behavior
WHERE dt >= '20260601'                              -- 根据实际日期范围调整
  AND dt <= '20260610'
GROUP BY dt
ORDER BY dt;


-- --------------------------------------------------------------------------
-- 3.2 按日期统计各风险标签人数 - 未成年专项视角
-- --------------------------------------------------------------------------
-- 用途: 聚焦未成年群体，展示防沉迷规则执行效果

SELECT
    dt                                              AS `日期`,
    COUNT(DISTINCT player_id)                       AS `未成年活跃数`,
    -- 未成年各风险标签人数
    COUNT(DISTINCT CASE WHEN risk_label = 0
        THEN player_id END)                         AS `正常`,
    COUNT(DISTINCT CASE WHEN risk_label = 1
        THEN player_id END)                         AS `超时预警`,
    COUNT(DISTINCT CASE WHEN risk_label = 2
        THEN player_id END)                         AS `夜间违规`,
    COUNT(DISTINCT CASE WHEN risk_label = 3
        THEN player_id END)                         AS `重度沉迷`,
    -- 未成年平均在线时长(分钟)
    ROUND(AVG(online_duration_min), 0)              AS `平均在线(分钟)`,
    -- 未成年最长在线时长(分钟)
    ROUND(MAX(online_duration_min), 0)              AS `最长在线(分钟)`,
    -- 未成年付费率
    ROUND(
        COUNT(DISTINCT CASE WHEN is_paying_player = 1 THEN player_id END)
        / NULLIF(COUNT(DISTINCT player_id), 0) * 100, 2
    )                                               AS `付费率(%)`,
    -- 未成年总充值金额
    ROUND(SUM(recharge_amount), 2)                  AS `总充值(元)`
FROM dwd_game_player_behavior
WHERE is_minor = 1                                  -- 仅未成年
  AND dt >= '20260601'
  AND dt <= '20260610'
GROUP BY dt
ORDER BY dt;


-- --------------------------------------------------------------------------
-- 3.3 未成年玩家在线时长分布统计 (按区间)
-- --------------------------------------------------------------------------
-- 用途: 了解未成年玩家在线时长模式，评估防沉迷效果

SELECT
    CASE
        WHEN online_duration_min < 30                            THEN 'A: <30分钟'
        WHEN online_duration_min >= 30  AND online_duration_min < 60  THEN 'B: 30~60分钟'
        WHEN online_duration_min >= 60  AND online_duration_min < 120 THEN 'C: 1~2小时'
        WHEN online_duration_min >= 120 AND online_duration_min < 180 THEN 'D: 2~3小时'
        WHEN online_duration_min >= 180 AND online_duration_min < 240 THEN 'E: 3~4小时'
        WHEN online_duration_min >= 240 AND online_duration_min < 300 THEN 'F: 4~5小时(超限)'
        ELSE                                                          'G: 5小时以上(严重超限)'
    END                                             AS `在线时长区间`,
    COUNT(DISTINCT player_id)                       AS `玩家数`,
    COUNT(*)                                        AS `行为记录数`,
    -- 区间占比
    ROUND(
        COUNT(DISTINCT player_id)
        / SUM(COUNT(DISTINCT player_id)) OVER() * 100, 2
    )                                               AS `占比(%)`,
    -- 区间内风险标签分布
    COUNT(DISTINCT CASE WHEN risk_label = 0 THEN player_id END) AS `正常`,
    COUNT(DISTINCT CASE WHEN risk_label = 1 THEN player_id END) AS `超时预警`,
    COUNT(DISTINCT CASE WHEN risk_label = 2 THEN player_id END) AS `夜间违规`,
    COUNT(DISTINCT CASE WHEN risk_label = 3 THEN player_id END) AS `重度沉迷`
FROM dwd_game_player_behavior
WHERE is_minor = 1
  AND dt = '20260610'                               -- 指定分析日期
GROUP BY 1
ORDER BY 1;


-- --------------------------------------------------------------------------
-- 3.4 夜间违规登录玩家明细查询 (标签2 + 标签3)
-- --------------------------------------------------------------------------
-- 用途: 导出夜间违规玩家清单，用于后续人工审核/通知监护人
-- 规则: 未成年 + 夜间(22:00-08:00)登录 = 二级违规或重度沉迷

SELECT
    dt                                              AS `日期`,
    player_id                                       AS `玩家ID`,
    game_id                                         AS `游戏ID`,
    login_time                                      AS `登录时间`,
    logout_time                                     AS `下线时间`,
    online_duration_min                             AS `在线时长(分钟)`,
    -- 格式化在线时长
    CONCAT(
        FLOOR(online_duration_min / 60), '小时',
        FLOOR(online_duration_min % 60), '分钟'
    )                                               AS `在线时长`,
    login_ip                                        AS `登录IP`,
    device_type                                     AS `设备类型`,
    game_region                                     AS `游戏区域`,
    match_count                                     AS `对局次数`,
    recharge_amount                                 AS `充值金额(元)`,
    -- 风险标签描述
    CASE risk_label
        WHEN 2 THEN '二级违规(仅夜间登录)'
        WHEN 3 THEN '重度沉迷(夜间+超时)'
        ELSE '其他'
    END                                             AS `风险类型`,
    -- 识别是否为重度情况
    IF(risk_label = 3, '是', '否')                  AS `是否重度沉迷`,
    -- 是否付费（夜间付费更敏感）
    IF(is_paying_player = 1, '是', '否')            AS `是否付费`
FROM dwd_game_player_behavior
WHERE is_minor = 1                                  -- 未成年
  AND risk_label IN (2, 3)                          -- 二级违规 或 重度沉迷
  AND dt >= '20260601'
  AND dt <= '20260610'
ORDER BY risk_label DESC, online_duration_min DESC
LIMIT 500;


-- --------------------------------------------------------------------------
-- 3.5 未成年夜间违规TopN - 按登录频次排序
-- --------------------------------------------------------------------------
-- 用途: 识别高频夜间违规玩家，重点干预

SELECT
    player_id                                       AS `玩家ID`,
    COUNT(DISTINCT dt)                              AS `违规天数`,
    COUNT(*)                                        AS `夜间登录次数`,
    ROUND(AVG(online_duration_min), 0)              AS `平均在线(分钟)`,
    ROUND(SUM(online_duration_min) / 60, 1)         AS `累计在线(小时)`,
    ROUND(SUM(recharge_amount), 2)                  AS `累计充值(元)`,
    -- 重度沉迷天数
    COUNT(DISTINCT CASE WHEN risk_label = 3
        THEN dt END)                                AS `重度沉迷天数`,
    -- 游戏分布
    COUNT(DISTINCT game_id)                         AS `游戏数`,
    -- 最近违规日期
    MAX(dt)                                         AS `最近违规日期`,
    -- 常用设备
    CONCAT_WS(',',
        COLLECT_SET(CASE WHEN rn = 1 THEN device_type END)
    )                                               AS `常用设备`
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY player_id
            ORDER BY online_duration_min DESC
        ) AS rn
    FROM dwd_game_player_behavior
    WHERE is_minor = 1
      AND risk_label IN (2, 3)
      AND dt >= '20260601' AND dt <= '20260610'
) t
GROUP BY player_id
ORDER BY `夜间登录次数` DESC, `累计在线(小时)` DESC
LIMIT 100;


-- --------------------------------------------------------------------------
-- 3.6 全维度交叉分析 - 按账号类型×时段×风险标签
-- --------------------------------------------------------------------------
-- 用途: 多维度分析看板，了解不同用户群体的行为特征

SELECT
    account_type                                    AS `账号类型`,
    login_period                                    AS `登录时段`,
    CASE risk_label
        WHEN 0 THEN '正常'
        WHEN 1 THEN '超时预警'
        WHEN 2 THEN '夜间违规'
        WHEN 3 THEN '重度沉迷'
    END                                             AS `风险等级`,
    device_type                                     AS `设备类型`,
    COUNT(DISTINCT player_id)                       AS `玩家数`,
    COUNT(*)                                        AS `记录数`,
    ROUND(AVG(online_duration_min), 0)              AS `平均在线(分钟)`,
    ROUND(AVG(match_count), 1)                      AS `平均对局数`,
    ROUND(AVG(recharge_amount), 2)                  AS `平均充值(元)`,
    ROUND(SUM(recharge_amount), 2)                  AS `总充值(元)`,
    -- 付费率
    ROUND(
        SUM(is_paying_player) / COUNT(*) * 100, 2
    )                                               AS `付费率(%)`
FROM dwd_game_player_behavior
WHERE dt = '20260610'
GROUP BY account_type, login_period, risk_label, device_type
ORDER BY
    account_type,
    FIELD(login_period, '白天', '傍晚', '夜间'),
    risk_label,
    device_type;


-- --------------------------------------------------------------------------
-- 3.7 防沉迷实时监控大屏核心SQL - 当日快照
-- --------------------------------------------------------------------------
-- 用途: 大屏展示当日实时防沉迷数据，每5分钟刷新

WITH today_data AS (
    SELECT *
    FROM dwd_game_player_behavior
    WHERE dt = DATE_FORMAT(NOW(), '%Y%m%d')         -- 当日分区
)
SELECT
    -- 概览指标
    COUNT(DISTINCT player_id)                       AS `当日活跃玩家`,
    COUNT(DISTINCT CASE WHEN is_minor = 1
        THEN player_id END)                         AS `当日未成年活跃`,
    COUNT(DISTINCT CASE WHEN risk_label > 0
        THEN player_id END)                         AS `当日风险玩家`,
    COUNT(DISTINCT CASE WHEN risk_label = 3
        THEN player_id END)                         AS `当日重度沉迷`,
    -- 占比
    ROUND(
        COUNT(DISTINCT CASE WHEN risk_label = 3 THEN player_id END)
        / NULLIF(COUNT(DISTINCT CASE WHEN is_minor = 1 THEN player_id END), 0) * 100, 2
    )                                               AS `未成年重度沉迷率(%)`,
    -- 行为指标
    ROUND(AVG(online_duration_min), 0)              AS `平均在线(分钟)`,
    ROUND(SUM(recharge_amount), 2)                  AS `当日总充值(元)`,
    -- 当前在线估算 (过去30分钟内有登录的玩家)
    COUNT(DISTINCT CASE
        WHEN login_time >= DATE_SUB(NOW(), INTERVAL 30 MINUTE)
        THEN player_id END
    )                                               AS `近30分钟活跃`,
    -- 各风险标签人数
    COUNT(DISTINCT CASE WHEN risk_label = 1
        THEN player_id END)                         AS `超时预警人数`,
    COUNT(DISTINCT CASE WHEN risk_label = 2
        THEN player_id END)                         AS `夜间违规人数`
FROM today_data;


-- --------------------------------------------------------------------------
-- 3.8 连续多日重度沉迷玩家检测 (连续3天以上)
-- --------------------------------------------------------------------------
-- 用途: 识别持续沉迷的高风险玩家，触发强制干预

WITH heavy_addiction_daily AS (
    -- 提取每日重度沉迷玩家 (risk_label=3)
    SELECT DISTINCT
        player_id,
        dt
    FROM dwd_game_player_behavior
    WHERE is_minor = 1
      AND risk_label = 3
      AND dt >= '20260601'
),
ranked_dates AS (
    -- 按玩家分组的日期排序
    SELECT
        player_id,
        dt,
        ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY dt) AS rn
    FROM heavy_addiction_daily
),
consecutive_groups AS (
    -- 识别连续日期组
    -- 原理: 日期序号 - 日期差值 = 常数 → 同一连续组
    SELECT
        player_id,
        dt,
        DATE_SUB(STR_TO_DATE(dt, '%Y%m%d'), INTERVAL rn DAY) AS grp_start
    FROM ranked_dates
),
streak_stats AS (
    -- 统计连续天数
    SELECT
        player_id,
        grp_start,
        COUNT(*)                                        AS consecutive_days,
        MIN(dt)                                         AS streak_start,
        MAX(dt)                                         AS streak_end
    FROM consecutive_groups
    GROUP BY player_id, grp_start
    HAVING consecutive_days >= 3                        -- 连续3天及以上
)
SELECT
    s.player_id                                     AS `玩家ID`,
    s.consecutive_days                              AS `连续重度沉迷天数`,
    s.streak_start                                  AS `开始日期`,
    s.streak_end                                    AS `结束日期`,
    -- 期间行为汇总
    ROUND(AVG(d.online_duration_min), 0)            AS `日均在线(分钟)`,
    ROUND(SUM(d.recharge_amount), 2)                AS `期间总充值(元)`,
    COUNT(DISTINCT d.game_id)                       AS `涉及游戏数`,
    -- 干预优先级
    CASE
        WHEN s.consecutive_days >= 7 THEN '⼀级干预(7天+)'
        WHEN s.consecutive_days >= 5 THEN '二级干预(5-6天)'
        ELSE '三级干预(3-4天)'
    END                                             AS `干预优先级`
FROM streak_stats s
JOIN dwd_game_player_behavior d
    ON s.player_id = d.player_id
    AND d.dt BETWEEN s.streak_start AND s.streak_end
    AND d.is_minor = 1
    AND d.risk_label = 3
GROUP BY s.player_id, s.consecutive_days, s.streak_start, s.streak_end
ORDER BY s.consecutive_days DESC, `期间总充值(元)` DESC
LIMIT 200;


-- ============================================================================
-- 第四部分: Doris 连接 Hive 配置要点
-- ============================================================================

-- --------------------------------------------------------------------------
-- 4.1 FE 配置 (fe.conf) - Hive Catalog 依赖
-- --------------------------------------------------------------------------
-- 需要将以下Hadoop/Hive配置文件放置到所有FE节点的 {DORIS_HOME}/conf/ 目录:
--   - core-site.xml     (HDFS配置，NameNode地址、HA等)
--   - hdfs-site.xml     (HDFS配置，HA故障转移等)
--   - hive-site.xml     (Hive Metastore URI等)
--
-- 或者通过 Catalog properties 直接指定 (见上方 CREATE CATALOG 语句)

-- --------------------------------------------------------------------------
-- 4.2 BE 配置 (be.conf) - HDFS 读取依赖
-- --------------------------------------------------------------------------
-- 需要将以下Hadoop/Hive配置文件放置到所有BE节点的 {DORIS_HOME}/conf/ 目录:
--   - core-site.xml
--   - hdfs-site.xml
--
-- 同时确保 BE 节点可以访问 HDFS DataNode (网络端口 50010/9866)

-- --------------------------------------------------------------------------
-- 4.3 环境变量配置
-- --------------------------------------------------------------------------
-- 在 fe.conf 和 be.conf 中添加:
--   JAVA_HOME = /usr/local/jdk
--   HADOOP_HOME = /usr/local/hadoop
--   HADOOP_CONF_DIR = /usr/local/hadoop/etc/hadoop

-- --------------------------------------------------------------------------
-- 4.4 验证 Hive Catalog 连接
-- --------------------------------------------------------------------------
-- 查询所有 Catalog
SHOW CATALOGS;

-- 切换到 Hive Catalog
SWITCH hive_catalog;

-- 列出 Hive 中的数据库
SHOW DATABASES;

-- 查询 Hive 表数据 (验证连通性)
SELECT COUNT(*) FROM game_anti_addiction.dwd_game_player_behavior;

-- 切回 Doris 内部 Catalog
SWITCH internal;

-- --------------------------------------------------------------------------
-- 4.5 同步性能优化建议
-- --------------------------------------------------------------------------
-- 1. 增大 INSERT 超时时间:
--    SET query_timeout = 3600;  (单位:秒)
--
-- 2. 并行同步多分区:
--    将多个 INSERT 语句放入一个事务或并发提交
--
-- 3. 调优 BE 节点参数:
--    be.conf 中增大: fragment_pool_thread_num_max = 4096
--
-- 4. 使用 Doris Stream Load 替代 INSERT (大量小分区场景):
--    curl --location-trusted -u root: \
--      -H "label:label_20260610" \
--      -H "format:orc" \
--      -T /path/to/data.orc \
--      http://fe_host:8030/api/game_anti_addiction/dwd_game_player_behavior/_stream_load

-- 5. Doris 写入调优 Session 变量:
SET enable_insert_strict = false;       -- 宽松模式，允许部分列默认值
SET exec_mem_limit = 8589934592;        -- 8GB内存限制
SET parallel_fragment_exec_instance_num = 4;  -- 并行执行实例数


-- ============================================================================
-- 完成提示
-- ============================================================================
-- 执行顺序:
--   1. 创建Doris表 → 2. 创建Hive Catalog → 3. 执行全量同步 → 4. 执行分析查询
--
-- 验证:
--   SELECT COUNT(*) FROM dwd_game_player_behavior;
--   SHOW PARTITIONS FROM dwd_game_player_behavior;
