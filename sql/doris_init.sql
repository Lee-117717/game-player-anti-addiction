-- ============================================================================
-- Apache Doris 单节点初始化SQL
-- 执行: mysql -h localhost -P 9030 -u root < doris_init.sql
-- ============================================================================

-- 1. 创建数据库
CREATE DATABASE IF NOT EXISTS game_anti_addiction;
USE game_anti_addiction;

-- 2. 创建 DWD 明细表 (DUPLICATE KEY 模型，单副本)
DROP TABLE IF EXISTS dwd_game_player_behavior;

CREATE TABLE IF NOT EXISTS dwd_game_player_behavior (
    dt                  DATE            COMMENT '分区日期',
    player_id           VARCHAR(64)     COMMENT '玩家ID',
    account_type        VARCHAR(10)     COMMENT '账号类型: minor/adult',
    game_id             VARCHAR(20)     COMMENT '游戏ID',
    login_time          DATETIME        COMMENT '登录时间',
    logout_time         DATETIME        COMMENT '下线时间',
    login_date          DATE            COMMENT '登录日期',
    login_hour          TINYINT         COMMENT '登录小时 0~23',
    login_dayofweek     TINYINT         COMMENT '登录星期几 1=周一',
    online_duration     INT             COMMENT '在线时长(秒)',
    online_duration_min DECIMAL(10,1)   COMMENT '在线时长(分钟)',
    match_count         SMALLINT        COMMENT '对局次数',
    recharge_amount     DECIMAL(12,2)   COMMENT '充值金额(元)',
    item_consumption    INT             COMMENT '道具消费数量',
    login_ip            VARCHAR(20)     COMMENT '登录IP',
    login_period        VARCHAR(10)     COMMENT '登录时段',
    is_night_login      TINYINT         COMMENT '是否夜间登录',
    game_region         VARCHAR(20)     COMMENT '游戏区域',
    device_type         VARCHAR(10)     COMMENT '设备类型',
    is_minor            TINYINT         COMMENT '是否未成年',
    is_heavy_gamer      TINYINT         COMMENT '是否重度玩家(>4h)',
    is_paying_player    TINYINT         COMMENT '是否付费玩家',
    risk_label          TINYINT         COMMENT '风险标签: 0-正常/1-预警/2-违规/3-重度',
    etl_time            DATETIME        COMMENT 'ETL时间',
    source_file         VARCHAR(256)    COMMENT '数据来源文件'
)
ENGINE = OLAP
DUPLICATE KEY (dt, player_id, account_type, game_id, login_time)
PARTITION BY RANGE (dt) ()
DISTRIBUTED BY HASH (player_id) BUCKETS 8
PROPERTIES (
    'replication_num' = '1',
    'compression' = 'LZ4',
    'dynamic_partition.enable' = 'true',
    'dynamic_partition.time_unit' = 'DAY',
    'dynamic_partition.start' = '-30',
    'dynamic_partition.end' = '3',
    'dynamic_partition.prefix' = 'p',
    'dynamic_partition.buckets' = '8'
);

-- 3. 创建初始分区
ALTER TABLE dwd_game_player_behavior
ADD PARTITION IF NOT EXISTS p20260610
VALUES [('20260610'), ('20260611'));

-- 4. 创建 ADS 聚合统计表
DROP TABLE IF EXISTS ads_anti_addiction_daily_stats;

CREATE TABLE IF NOT EXISTS ads_anti_addiction_daily_stats (
    dt                  DATE        COMMENT '统计日期',
    dau                 BIGINT      COMMENT '日活DAU',
    total_records       BIGINT      COMMENT '总行为记录数',
    paying_players      BIGINT      COMMENT '付费玩家数',
    total_players       BIGINT      COMMENT '总玩家数',
    total_recharge      DECIMAL(16,2) COMMENT '总充值金额',
    minor_players       BIGINT      COMMENT '未成年玩家数',
    risk_label_0        BIGINT      COMMENT '正常玩家数',
    risk_label_1        BIGINT      COMMENT '一级预警人数',
    risk_label_2        BIGINT      COMMENT '二级违规人数',
    risk_label_3        BIGINT      COMMENT '重度沉迷人数',
    avg_online_min      DECIMAL(10,1) COMMENT '平均在线时长(分钟)',
    etl_time            DATETIME    COMMENT 'ETL时间'
)
ENGINE = OLAP
DUPLICATE KEY (dt)
PARTITION BY RANGE (dt) ()
DISTRIBUTED BY HASH (dt) BUCKETS 4
PROPERTIES (
    'replication_num' = '1',
    'compression' = 'LZ4',
    'dynamic_partition.enable' = 'true',
    'dynamic_partition.time_unit' = 'DAY',
    'dynamic_partition.start' = '-90',
    'dynamic_partition.end' = '3',
    'dynamic_partition.prefix' = 'p',
    'dynamic_partition.buckets' = '4'
);
