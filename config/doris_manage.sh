#!/bin/bash
# ============================================================================
# Apache Doris 单节点管理脚本
# 用法: sh doris_manage.sh {start|stop|status|init|sync [date]}
# ============================================================================
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.412.b08-1.el7_9.x86_64
DORIS_HOME=/home/hadoop/data/doris

case "$1" in
  start)
    echo "Starting Apache Doris..."
    echo "[1/2] Starting FE..."
    $DORIS_HOME/fe/bin/start_fe.sh --daemon 2>&1
    sleep 5

    echo "[2/2] Starting BE..."
    $DORIS_HOME/be/bin/start_be.sh --daemon 2>&1
    sleep 5

    echo ""
    echo "=== Doris Status ==="
    jps 2>/dev/null | grep -E "DorisFE|DorisBE|PaloFe|StarrocksBe"
    echo "FE HTTP: http://localhost:8030"
    echo "BE HTTP: http://localhost:8040"
    echo "FE Query Port: 9030"
    ;;

  stop)
    echo "Stopping Apache Doris..."
    echo "[1/2] Stopping BE..."
    $DORIS_HOME/be/bin/stop_be.sh 2>&1
    echo "[2/2] Stopping FE..."
    $DORIS_HOME/fe/bin/stop_fe.sh 2>&1
    echo "Doris stopped."
    ;;

  status)
    echo "=== Doris Status ==="
    echo -n "FE: "; jps 2>/dev/null | grep -q "DorisFE\|PaloFe" && echo "RUNNING" || echo "STOPPED"
    echo -n "BE: "; jps 2>/dev/null | grep -q "DorisBE\|StarrocksBe" && echo "RUNNING" || echo "STOPPED"
    echo ""
    echo "FE Web: http://localhost:8030"
    echo "BE Web: http://localhost:8040"
    ;;

  init)
    echo "=== Doris First-Time Initialization ==="
    echo ""
    echo "Step 1: Register BE to FE cluster..."
    # 等待FE启动完成
    sleep 3
    mysql -h localhost -P 9030 -u root --skip-column-names -e \
        "ALTER SYSTEM ADD BACKEND '127.0.0.1:9050';" 2>&1
    sleep 2
    echo ""
    echo "Step 2: Check backend status..."
    mysql -h localhost -P 9030 -u root --skip-column-names -e \
        "SHOW BACKENDS\G" 2>&1 | grep -E "Host|HeartbeatPort|Alive|TotalCapacity"
    echo ""
    echo "Step 3: Create database and tables..."
    mysql -h localhost -P 9030 -u root < \
        /home/hadoop/game_player_anti_addiction/doris/doris_init.sql 2>&1
    echo ""
    echo "=== Init complete ==="
    ;;

  sync)
    DT=${2:-20260610}
    echo "=== Syncing Hive DWD data to Doris for date: $DT ==="
    echo ""
    echo "Note: Data sync requires Hive Catalog setup."
    echo "For single-node demo, using Spark-based export/import instead."
    echo ""
    source /home/hadoop/game_player_anti_addiction/env.sh

    # Use Spark to export Hive DWD data as CSV, then load via curl Stream Load
    echo "[1/3] Exporting Hive DWD data to CSV..."
    $SPARK_HOME/bin/spark-sql --master local[2] --driver-memory 512m \
        --conf spark.sql.catalogImplementation=hive \
        -e "USE game_anti_addiction; \
            SELECT \
              player_id, account_type, game_id, \
              CAST(login_time AS STRING), CAST(logout_time AS STRING), \
              CAST(login_date AS STRING), \
              login_hour, login_dayofweek, \
              online_duration, online_duration_min, \
              match_count, recharge_amount, item_consumption, \
              login_ip, login_period, is_night_login, \
              game_region, device_type, \
              is_minor, is_heavy_gamer, is_paying_player, \
              risk_label, \
              CAST(etl_time AS STRING), source_file \
            FROM dwd_game_player_behavior \
            WHERE dt='$DT';" \
        2>&1 | grep -v "WARN\|INFO\|Time taken\|spark-sql\|To adjust\|Spark mas" \
        > /tmp/doris_sync_${DT}.csv

    ROWS=$(wc -l < /tmp/doris_sync_${DT}.csv)
    echo "  Exported $ROWS rows"

    echo "[2/3] Loading into Doris via Stream Load..."
    curl --location-trusted -u root: \
        -H "label:sync_${DT}" \
        -H "column_separator:\t" \
        -H "columns:player_id,account_type,game_id,login_time,logout_time,login_date,login_hour,login_dayofweek,online_duration,online_duration_min,match_count,recharge_amount,item_consumption,login_ip,login_period,is_night_login,game_region,device_type,is_minor,is_heavy_gamer,is_paying_player,risk_label,etl_time,source_file,dt='$DT'" \
        -H "format:csv" \
        -T /tmp/doris_sync_${DT}.csv \
        http://localhost:8030/api/game_anti_addiction/dwd_game_player_behavior/_stream_load 2>&1

    echo ""
    echo "[3/3] Verifying sync..."
    mysql -h 127.0.0.1 -P 9030 -u root --skip-column-names -e \
        "SELECT COUNT(*) AS synced_rows FROM game_anti_addiction.dwd_game_player_behavior WHERE dt='$DT';" 2>&1
    echo ""
    echo "=== Sync complete ==="
    ;;

  query)
    echo "=== Quick Anti-Addiction Query ==="
    echo ""
    mysql -h localhost -P 9030 -u root --table -e "
    SELECT
        risk_label,
        CASE risk_label
            WHEN 0 THEN '正常玩家'
            WHEN 1 THEN '一级预警(>4h)'
            WHEN 2 THEN '二级违规(夜间)'
            WHEN 3 THEN '重度沉迷'
        END AS risk_desc,
        COUNT(*) AS record_count,
        COUNT(DISTINCT player_id) AS player_count,
        ROUND(AVG(online_duration_min), 0) AS avg_online_min,
        ROUND(SUM(recharge_amount), 2) AS total_recharge
    FROM game_anti_addiction.dwd_game_player_behavior
    WHERE dt = '${2:-20260610}'
    GROUP BY risk_label
    ORDER BY risk_label;
    " 2>&1
    ;;

  *)
    echo "用法: sh doris_manage.sh {start|stop|status|init|sync [date]|query [date]}"
    echo ""
    echo "  start  - 启动 Doris FE + BE"
    echo "  stop   - 停止 Doris"
    echo "  status - 查看状态"
    echo "  init   - 首次初始化 (添加BE + 建表)"
    echo "  sync   - 同步Hive DWD数据到Doris"
    echo "  query  - 快速防沉迷查询"
    ;;
esac
