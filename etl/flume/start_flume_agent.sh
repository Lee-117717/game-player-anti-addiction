#!/bin/bash
# ============================================================================
# 游戏平台玩家行为分析与防沉迷系统
# Flume Agent 启动脚本 (exec → memory → Kafka + HDFS 双链路)
# ============================================================================
# 前置条件:
#   1. Kafka 已启动 (sh /home/hadoop/data/kafka/start_kafka.sh)
#   2. Hadoop HDFS 已启动 (start-dfs.sh)
#   3. /tmp/game_player.log 日志文件存在
# ============================================================================
set -e

export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.412.b08-1.el7_9.x86_64}

# ---- 配置 ----
FLUME_HOME=${FLUME_HOME:-/home/hadoop/data/flume}
PROJECT_DIR=$(cd "$(dirname "$0")/.." && pwd)
CONF_FILE="${PROJECT_DIR}/03_flume_config/game_flume.conf"
AGENT_NAME="a1"

echo "=========================================="
echo "  启动 Flume Agent — 双链路采集"
echo "=========================================="
echo "  Agent名称 : ${AGENT_NAME}"
echo "  配置文件  : ${CONF_FILE}"
echo "  Flume目录 : ${FLUME_HOME}"
echo "  JAVA_HOME : ${JAVA_HOME}"
echo "=========================================="

# 检查 Flume 安装
if [ ! -f "${FLUME_HOME}/bin/flume-ng" ]; then
    echo "[ERROR] Flume 未找到: ${FLUME_HOME}/bin/flume-ng"
    echo "  请安装 Flume 1.9.0+ 并设置 FLUME_HOME 环境变量"
    exit 1
fi

# 检查配置文件
if [ ! -f "${CONF_FILE}" ]; then
    echo "[ERROR] 配置文件不存在: ${CONF_FILE}"
    exit 1
fi

# 检查 Kafka 是否运行
if ! ps aux | grep -q "[k]afka.Kafka"; then
    echo "[WARN] Kafka 未运行，Kafka Sink 将失败"
    echo "  启动 Kafka: sh /home/hadoop/data/kafka/start_kafka.sh"
fi

# 检查 HDFS 是否运行
if ! jps 2>/dev/null | grep -q "NameNode"; then
    echo "[WARN] HDFS NameNode 未运行，HDFS Sink 将失败"
fi

# 检查日志源文件
LOG_SOURCE="${PROJECT_DIR}/logs/game_player_behavior.log"
if [ ! -f "${LOG_SOURCE}" ]; then
    echo "[WARN] 日志源文件不存在: ${LOG_SOURCE}"
    echo "  请先运行: python3 ${PROJECT_DIR}/generate_game_logs.py 1000 0"
fi

echo ""
echo "  启动方式:"
echo "  [1] 前台运行 (Ctrl+C 停止):"
echo "      sh ${0}"
echo ""
echo "  [2] 后台运行 (推荐):"
echo "      nohup sh ${0} --background &"
echo ""

if [ "$1" = "--background" ]; then
    # 后台模式
    LOG_DIR="${PROJECT_DIR}/03_flume_config/logs"
    mkdir -p ${LOG_DIR}
    nohup ${FLUME_HOME}/bin/flume-ng agent \
        --name ${AGENT_NAME} \
        --conf ${FLUME_HOME}/conf \
        --conf-file ${CONF_FILE} \
        -Dflume.root.logger=INFO,console \
        -Dflume.monitoring.type=http \
        -Dflume.monitoring.port=34545 \
        -Duser.timezone=Asia/Shanghai \
        > ${LOG_DIR}/flume_output.log 2>&1 &

    FLUME_PID=$!
    echo "  Flume PID: ${FLUME_PID}"
    echo "  日志输出: ${LOG_DIR}/flume_output.log"
    echo "  监控地址: http://localhost:34545/metrics"
    sleep 3
    if kill -0 ${FLUME_PID} 2>/dev/null; then
        echo "  [OK] Flume Agent 启动成功"
    else
        echo "  [FAIL] Flume Agent 启动失败，查看日志:"
        tail -20 ${LOG_DIR}/flume_output.log
    fi
else
    # 前台模式
    exec ${FLUME_HOME}/bin/flume-ng agent \
        --name ${AGENT_NAME} \
        --conf ${FLUME_HOME}/conf \
        --conf-file ${CONF_FILE} \
        -Dflume.root.logger=INFO,console \
        -Dflume.monitoring.type=http \
        -Dflume.monitoring.port=34545 \
        -Duser.timezone=Asia/Shanghai
fi
