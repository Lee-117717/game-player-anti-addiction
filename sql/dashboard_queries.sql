-- ============================================================================
-- 游戏防沉迷监控大屏 — 全部8组图表SQL语句
-- 数据源: Apache Doris (dwd_game_player_behavior)
-- 所有查询以 #{latestDt} = SELECT MAX(dt) FROM dwd_game_player_behavior 为参数
-- ============================================================================

-- ============================================================================
-- 接口1: KPI指标卡 (GET /api/kpi/today)
-- 图表类型: 6个数字卡片 (当日活跃/未成年活跃/总充值/风险玩家/重度沉迷/平均在线)
-- 前端位置: Dashboard.vue 第1-2行, 2行×3列
-- ============================================================================
SELECT
    COUNT(DISTINCT player_id)                       AS total_active,
    COUNT(DISTINCT CASE WHEN is_minor = 1
        THEN player_id END)                         AS minor_active,
    COUNT(DISTINCT CASE WHEN risk_label > 0
        THEN player_id END)                         AS at_risk_count,
    COUNT(DISTINCT CASE WHEN risk_label = 3
        THEN player_id END)                         AS severe_count,
    ROUND(SUM(recharge_amount), 2)                  AS total_recharge,
    ROUND(AVG(online_duration_min), 0)              AS avg_online_min
FROM dwd_game_player_behavior
WHERE dt = #{latestDt};


-- ============================================================================
-- 接口2: 在线时长分布 (GET /api/duration/dist)
-- 图表类型: 南丁格尔玫瑰图
-- 前端位置: Dashboard.vue 第3行左
-- ============================================================================
SELECT
    CASE
        WHEN online_duration_min < 30   THEN '<30分钟'
        WHEN online_duration_min < 60   THEN '30~60分钟'
        WHEN online_duration_min < 120  THEN '1~2小时'
        WHEN online_duration_min < 180  THEN '2~3小时'
        WHEN online_duration_min < 240  THEN '3~4小时'
        ELSE                                 '4小时以上(超限)'
    END                                               AS duration_range,
    COUNT(DISTINCT player_id)                         AS player_count,
    COUNT(*)                                          AS record_count,
    ROUND(COUNT(DISTINCT player_id) * 100.0
        / SUM(COUNT(DISTINCT player_id)) OVER(), 1)   AS pct
FROM dwd_game_player_behavior
WHERE dt = #{latestDt}
GROUP BY duration_range
ORDER BY MIN(online_duration_min);


-- ============================================================================
-- 接口3: 7天风险趋势 (GET /api/risk/trend)
-- 图表类型: 堆叠柱状+折线双轴组合图
-- 前端位置: Dashboard.vue 第3行右
-- ============================================================================
SELECT
    dt                                                AS stat_date,
    COUNT(DISTINCT player_id)                         AS dau,
    COUNT(DISTINCT CASE WHEN risk_label = 1
        THEN player_id END)                           AS level1_count,
    COUNT(DISTINCT CASE WHEN risk_label = 2
        THEN player_id END)                           AS level2_count,
    COUNT(DISTINCT CASE WHEN risk_label = 3
        THEN player_id END)                           AS level3_count,
    ROUND(
        COUNT(DISTINCT CASE WHEN risk_label > 0 THEN player_id END)
        * 100.0 / NULLIF(COUNT(DISTINCT player_id), 0), 1
    )                                                 AS risk_rate_pct,
    ROUND(
        COUNT(DISTINCT CASE WHEN risk_label > 0 AND is_minor = 1
            THEN player_id END)
        * 100.0 / NULLIF(COUNT(DISTINCT CASE WHEN is_minor = 1
            THEN player_id END), 0), 1
    )                                                 AS minor_risk_rate_pct
FROM dwd_game_player_behavior
WHERE dt >= DATE_SUB(#{latestDt}, INTERVAL 6 DAY)
  AND dt <= #{latestDt}
GROUP BY dt
ORDER BY dt;


-- ============================================================================
-- 接口4: 付费分析 (GET /api/payment/analysis)
-- 图表类型: 桑基图 (账号类型 → 付费区间)
-- 前端位置: Dashboard.vue 第4行右
-- ============================================================================
SELECT
    account_type                                      AS account_type,
    CASE
        WHEN recharge_amount = 0                      THEN '未付费'
        WHEN recharge_amount <= 30                    THEN '1~30元'
        WHEN recharge_amount <= 100                   THEN '30~100元'
        WHEN recharge_amount <= 300                   THEN '100~300元'
        WHEN recharge_amount <= 1000                  THEN '300~1000元'
        ELSE                                               '1000元以上'
    END                                               AS recharge_range,
    COUNT(DISTINCT player_id)                         AS player_count,
    ROUND(SUM(recharge_amount), 2)                    AS total_amount,
    ROUND(AVG(recharge_amount), 2)                    AS avg_amount
FROM dwd_game_player_behavior
WHERE dt = #{latestDt}
GROUP BY account_type, recharge_range
ORDER BY account_type, MIN(recharge_amount);


-- ============================================================================
-- 接口5: 24小时时段登录统计 (GET /api/period/stats)
-- 图表类型: 堆叠面积图 (成年/未成年)
-- 前端位置: Dashboard.vue 第4行左
-- ============================================================================
SELECT
    login_hour                                        AS hour_of_day,
    COUNT(DISTINCT CASE WHEN account_type = 'adult'
        THEN player_id END)                           AS adult_active,
    COUNT(DISTINCT CASE WHEN account_type = 'minor'
        THEN player_id END)                           AS minor_active,
    COUNT(DISTINCT player_id)                         AS total_active,
    IF(login_hour >= 22 OR login_hour < 8, '夜间', '非夜间')
                                                      AS night_flag
FROM dwd_game_player_behavior
WHERE dt = #{latestDt}
GROUP BY login_hour
ORDER BY login_hour;


-- ============================================================================
-- 接口6: 高风险玩家Top10 (GET /api/risk/ranking)
-- 图表类型: 排行表格 (可点击行下钻到夜间违规明细)
-- 前端位置: Dashboard.vue 第5行左
-- ============================================================================
SELECT
    player_id                                         AS player_id,
    COUNT(DISTINCT dt)                                AS active_days,
    ROUND(SUM(online_duration_min) / 60, 1)           AS total_hours,
    ROUND(AVG(online_duration_min), 0)                AS avg_daily_min,
    SUM(recharge_amount)                              AS total_recharge,
    MAX(risk_label)                                   AS max_risk_level,
    COUNT(DISTINCT game_id)                           AS game_count
FROM dwd_game_player_behavior
WHERE is_minor = 1
  AND risk_label > 0
  AND dt >= DATE_SUB(#{latestDt}, INTERVAL 6 DAY)
GROUP BY player_id
ORDER BY total_hours DESC
LIMIT 10;


-- ============================================================================
-- 接口7: 违规玩家实时明细 (GET /api/violation/detail)
-- 图表类型: 自动滚动表格
-- 前端位置: Dashboard.vue 第5行右
-- ============================================================================
SELECT
    dt                                                AS stat_date,
    player_id,
    game_id,
    login_time,
    logout_time,
    ROUND(online_duration_min, 0)                     AS online_min,
    login_ip,
    device_type,
    game_region,
    CASE risk_label
        WHEN 1 THEN '超时预警'
        WHEN 2 THEN '夜间违规'
        WHEN 3 THEN '重度沉迷'
    END                                               AS risk_type,
    recharge_amount,
    match_count
FROM dwd_game_player_behavior
WHERE risk_label > 0
  AND dt >= DATE_SUB(#{latestDt}, INTERVAL 1 DAY)
ORDER BY risk_label DESC, online_duration_min DESC
LIMIT 100;


-- ============================================================================
-- 接口8: 未成年夜间违规明细 (GET /api/minor/night-detail)
-- 图表类型: 弹窗表格 (点击TOP10某行后弹出, 前端按playerId过滤)
-- 前端位置: Dashboard.vue 弹窗 DetailModal
-- ============================================================================
SELECT
    dt,
    player_id,
    login_time,
    logout_time,
    ROUND(online_duration_min, 0)                     AS online_min,
    login_hour,
    device_type,
    game_region,
    game_id,
    recharge_amount,
    CASE risk_label
        WHEN 2 THEN '二级违规(仅夜间)'
        WHEN 3 THEN '重度沉迷(夜间+超时)'
    END                                               AS risk_desc
FROM dwd_game_player_behavior
WHERE is_minor = 1
  AND risk_label IN (2, 3)
  AND dt >= DATE_SUB(#{latestDt}, INTERVAL 6 DAY)
ORDER BY dt DESC, online_duration_min DESC;
