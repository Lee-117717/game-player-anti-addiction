#!/bin/bash
# ============================================================================
# 游戏平台玩家行为分析与防沉迷系统
# Flume Agent 启动脚本
# ============================================================================
# 使用说明:
#   1. 修改 FLUME_HOME 为实际Flume安装路径
#   2. 修改下方Kafka和HDFS连接参数(或直接修改flume.conf)
#   3. 执行: sh start_flume.sh
#   4. 后台运行: nohup sh start_flume.sh > flume.log 2>&1 &
# ============================================================================

set -e

# ---- 基础配置(根据实际环境修改) ----
FLUME_HOME=${FLUME_HOME:-/usr/local/flume}           # Flume安装目录
CONF_DIR=$(cd "$(dirname "$0")" && pwd)                # 配置文件所在目录
AGENT_NAME="a1"                                        # Agent名称(与flume.conf中一致)

# ---- JVM参数 ----
# 堆内存根据数据量和机器配置调整，建议2GB-4GB
JAVA_OPTS="-Xms1024m -Xmx2048m"
JAVA_OPTS="${JAVA_OPTS} -XX:+UseG1GC"
JAVA_OPTS="${JAVA_OPTS} -XX:MaxGCPauseMillis=200"
JAVA_OPTS="${JAVA_OPTS} -XX:+PrintGCDetails"
JAVA_OPTS="${JAVA_OPTS} -XX:+PrintGCDateStamps"
JAVA_OPTS="${JAVA_OPTS} -Xloggc:${CONF_DIR}/gc.log"
# 时区设置，影响HDFS目录中的日期
JAVA_OPTS="${JAVA_OPTS} -Duser.timezone=Asia/Shanghai"

# ---- 依赖JAR包路径(如需自定义) ----
# Kafka Sink依赖kafka-clients包，确保在$FLUME_HOME/lib/下存在
# 如缺失: cp $KAFKA_HOME/libs/kafka-clients-*.jar $FLUME_HOME/lib/
# HDFS Sink依赖hadoop-common/hadoop-hdfs，通常Flume自带

# ---- 日志配置 ----
export FLUME_LOG_DIR=${FLUME_HOME}/logs

# ---- 启动命令 ----
echo "=========================================="
echo "  启动 Flume Agent"
echo "=========================================="
echo "  Agent名称 : ${AGENT_NAME}"
echo "  配置文件  : ${CONF_DIR}/flume.conf"
echo "  Flume目录 : ${FLUME_HOME}"
echo "=========================================="

exec ${FLUME_HOME}/bin/flume-ng agent \
    --name ${AGENT_NAME} \
    --conf ${FLUME_HOME}/conf \
    --conf-file ${CONF_DIR}/flume.conf \
    -Dflume.root.logger=INFO,console \
    -Dflume.monitoring.type=http \
    -Dflume.monitoring.port=34545 \
    ${JAVA_OPTS}
