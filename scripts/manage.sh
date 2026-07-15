#!/bin/bash
# ============================================================================
# 游戏平台玩家行为分析 — 一键管理脚本
# 用法: sh manage.sh {start|stop|status|test|etl|etl-local}
# 注意: YARN模式分布式调度受限于虚拟机资源，暂未完成测试
#       Spark ETL 使用 local[1] 模式已验证通过
# ============================================================================
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.412.b08-1.el7_9.x86_64

case "$1" in
  start)
    echo "=========================================="
    echo "  启动全部服务 (HDFS + YARN + Kafka + Flume)"
    echo "=========================================="
    echo ""
    echo "[1/4] Starting Hadoop HDFS..."
    /home/hadoop/data/hadoop/sbin/start-dfs.sh 2>&1 | tail -1
    sleep 3

    echo "[2/4] Starting YARN..."
    /home/hadoop/data/hadoop/sbin/start-yarn.sh 2>&1 | tail -2
    sleep 3

    echo "[3/4] Starting Kafka..."
    sh /home/hadoop/data/kafka/start_kafka.sh 2>&1 | grep "OK\|FAIL\|WARN"
    sleep 3

    echo "[4/4] Starting Flume..."
    nohup /home/hadoop/data/flume/bin/flume-ng agent \
        --name a1 \
        --conf /home/hadoop/data/flume/conf \
        --conf-file /home/hadoop/game_player_anti_addiction/03_flume_config/game_flume.conf \
        -Dflume.monitoring.type=http \
        -Dflume.monitoring.port=34545 \
        -Duser.timezone=Asia/Shanghai \
        > /home/hadoop/game_player_anti_addiction/03_flume_config/logs/flume_output.log 2>&1 &
    sleep 5
    ps aux | grep -q "[f]lume.node.Application" && echo "  Flume: OK" || echo "  Flume: check log"
    echo ""
    echo "=== All services started ==="
    ;;

  stop)
    echo "Stopping all services..."
    echo "[1/4] Stopping Flume..."
    pkill -f flume.node.Application 2>/dev/null && echo "  Flume: stopped" || echo "  Flume: not running"
    echo "[2/4] Stopping Kafka..."
    sh /home/hadoop/data/kafka/stop_kafka.sh 2>&1 | grep "OK\|stopped"
    echo "[3/4] Stopping YARN..."
    /home/hadoop/data/hadoop/sbin/stop-yarn.sh 2>&1 | tail -2
    echo "[4/4] Stopping Hadoop HDFS..."
    /home/hadoop/data/hadoop/sbin/stop-dfs.sh 2>&1 | tail -1
    echo "=== All services stopped ==="
    ;;

  status)
    echo "=== Service Status ==="
    echo -n "HDFS:  "; jps 2>/dev/null | grep -q "NameNode" && echo "RUNNING" || echo "STOPPED"
    echo -n "YARN:  "; jps 2>/dev/null | grep -q "ResourceManager" && echo "RUNNING" || echo "STOPPED"
    echo -n "Kafka: "; ps aux | grep -q "[k]afka.Kafka" && echo "RUNNING" || echo "STOPPED"
    echo -n "Flume: "; ps aux | grep -q "[f]lume.node.Application" && echo "RUNNING" || echo "STOPPED"
    echo ""
    echo "=== HDFS ==="
    hdfs dfsadmin -report 2>/dev/null | grep -E "Live|Capacity|Used"
    echo ""
    echo "=== YARN ==="
    echo "  Web UI: http://localhost:8088"
    jps 2>/dev/null | grep -E "ResourceManager|NodeManager"
    echo ""
    echo "=== Kafka ==="
    /home/hadoop/data/kafka/bin/kafka-topics.sh --describe --topic game-player-log --bootstrap-server localhost:9092 2>/dev/null | head -4
    echo ""
    echo "=== Disk ==="
    df -h /home/hadoop/data
    ;;

  test)
    sh /home/hadoop/game_player_anti_addiction/03_flume_config/test_pipeline.sh
    ;;

  etl-local)
    echo "=========================================="
    echo "  Spark ETL (Local[1] 模式) — 虚拟机适配版"
    echo "=========================================="
    source /home/hadoop/game_player_anti_addiction/env.sh
    # 注: Spark最低要求~450MB堆内存，256m无法启动，使用512m
    $SPARK_HOME/bin/spark-submit \
        --master local[1] \
        --driver-memory 512m \
        --conf spark.sql.adaptive.enabled=false \
        --conf spark.sql.adaptive.coalescePartitions.enabled=false \
        --conf spark.sql.shuffle.partitions=2 \
        /home/hadoop/game_player_anti_addiction/spark/etl_anti_addiction.py ${2:-20260610}
    ;;

  etl)
    echo "=========================================="
    echo "  Spark ETL on YARN — 受限于虚拟机资源"
    echo "  分布式调度测试暂未完成，建议使用 etl-local"
    echo "=========================================="
    source /home/hadoop/game_player_anti_addiction/env.sh
    $SPARK_HOME/bin/spark-submit \
        --master yarn \
        --deploy-mode client \
        --driver-memory 512m \
        --executor-memory 512m \
        --executor-cores 1 \
        --num-executors 1 \
        --conf spark.yarn.am.memory=256m \
        /home/hadoop/game_player_anti_addiction/spark/etl_anti_addiction.py ${2:-20260610}
    ;;

  *)
    echo "用法: sh manage.sh {start|stop|status|test|etl|etl-local [date]}"
    echo ""
    echo "  start     - 启动 Hadoop HDFS + YARN + Kafka + Flume"
    echo "  stop      - 停止所有服务"
    echo "  status    - 查看服务状态"
    echo "  test      - 运行端到端链路测试"
    echo "  etl-local - 运行 Spark ETL (local[1], 256m 内存, 虚拟机适配)"
    echo "  etl       - 运行 Spark ETL on YARN (受限于虚拟机资源，暂未完成分布式测试)"
    ;;
esac
