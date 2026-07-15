#!/bin/bash
# ============================================================================
# 游戏平台行为分析与防沉迷系统 — 端到端链路测试
# 数据流: /tmp/game_player.log → Flume exec → memory → Kafka + HDFS
# ============================================================================
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.412.b08-1.el7_9.x86_64

KAFKA_HOME=/home/hadoop/data/kafka
TOPIC=game-player-log
BOOTSTRAP=localhost:9092
TEST_LOG=/home/hadoop/game_player_anti_addiction/logs/game_player_behavior.log
FLUME_METRICS=http://localhost:34545/metrics

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass() { echo -e "${GREEN}[PASS]${NC} $1"; }
fail() { echo -e "${RED}[FAIL]${NC} $1"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }

echo ""
echo "======================================================"
echo "  游戏玩家行为分析 — 端到端链路测试"
echo "  /tmp/game_player.log → Flume → Kafka + HDFS"
echo "======================================================"

# ---- 1. 环境检查 ----
echo ""
echo "--- [1/5] 环境检查 ---"

java -version 2>&1 | grep -q "1.8" && pass "Java 1.8" || fail "Java 1.8"

if ps aux | grep -q "[k]afka.Kafka"; then
    pass "Kafka 运行中"
else
    fail "Kafka 未运行"
fi

if ${KAFKA_HOME}/bin/kafka-topics.sh --list --bootstrap-server ${BOOTSTRAP} 2>/dev/null | grep -q "^${TOPIC}$"; then
    pass "Topic '${TOPIC}' 已存在"
else
    fail "Topic '${TOPIC}' 未创建"
    echo "       修复: ${KAFKA_HOME}/bin/kafka-topics.sh --create --topic ${TOPIC} --bootstrap-server ${BOOTSTRAP} --partitions 3 --replication-factor 1"
fi

curl -s ${FLUME_METRICS} > /dev/null 2>&1 && pass "Flume 监控端点正常" || warn "Flume 未启动 (端口 34545)"

if jps 2>/dev/null | grep -q "NameNode"; then
    pass "HDFS 可用"
else
    warn "HDFS 不可用 — 仅测试 Kafka 链路"
fi

# ---- 2. 日志源文件 ----
echo ""
echo "--- [2/5] 日志源文件 ---"

if [ -f "${TEST_LOG}" ]; then
    LINES=$(wc -l < ${TEST_LOG})
    pass "源文件存在: ${TEST_LOG} (${LINES} 行)"
    echo "       最新一行: $(tail -1 ${TEST_LOG} | head -c 120)"
else
    warn "源文件不存在: ${TEST_LOG}"
    echo "       创建空文件 + 写入初始数据..."
    printf '{"user_id":"init","action":"system_start","timestamp":"%s"}\n' "$(date +%Y-%m-%dT%H:%M:%S)" > ${TEST_LOG}
    pass "已创建源文件"
fi

# ---- 3. Kafka 链路测试 ----
echo ""
echo "--- [3/5] Kafka 链路测试 ---"

# 记录测试前的 topic offset
BEFORE=$(${KAFKA_HOME}/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
    --topic ${TOPIC} --bootstrap-server ${BOOTSTRAP} --time -1 2>/dev/null | \
    awk -F: '{s+=$NF} END{print s+0}')
echo "      测试前 Topic 总 offset: ${BEFORE}"

# 写入一条带唯一标记的测试日志
TEST_ID="link_test_$(date +%s)_$$"
    printf '{"user_id":"%s","action":"login","platform":"mobile","duration":0,"timestamp":"%s"}\n' "${TEST_ID}" "$(date +%Y-%m-%dT%H:%M:%S)" >> ${TEST_LOG}
echo "      已写入测试日志 (ID=${TEST_ID})"

# 等待 Flume exec source 采集 (batchTimeout=3000ms + 余量)
echo "      等待 Flume 采集 (6s)..."
sleep 6

AFTER=$(${KAFKA_HOME}/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
    --topic ${TOPIC} --bootstrap-server ${BOOTSTRAP} --time -1 2>/dev/null | \
    awk -F: '{s+=$NF} END{print s+0}')
echo "      测试后 Topic 总 offset: ${AFTER}"

if [ "${AFTER}" -gt "${BEFORE}" ]; then
    DELTA=$((AFTER - BEFORE))
    pass "Kafka 链路正常 — 新增 ${DELTA} 条消息"

    # 消费最新消息验证内容
    echo "      尝试消费最新 2 条消息:"
    ${KAFKA_HOME}/bin/kafka-console-consumer.sh \
        --topic ${TOPIC} --bootstrap-server ${BOOTSTRAP} \
        --max-messages 2 --timeout-ms 8000 2>/dev/null | while read msg; do
        echo "      | ${msg:0:150}"
        if echo "$msg" | grep -q "${TEST_ID}"; then
            echo "      $(pass '测试消息已确认到达 Kafka')"
        fi
    done
else
    fail "Kafka 链路异常 — 未检测到新消息"
    echo "      排查: (1) Flume 是否运行 (2) exec source 是否正确配置"
    echo "      手动测试: echo 'test' >> ${TEST_LOG} && 等待5s后检查 Kafka"
fi

# ---- 4. HDFS 链路测试 ----
echo ""
echo "--- [4/5] HDFS 链路检查 ---"

if jps 2>/dev/null | grep -q "NameNode"; then
    TODAY=$(date +%Y%m%d)
    HDFS_PATH="/user/flume/game_logs/dt=${TODAY}"
    if hdfs dfs -test -d "${HDFS_PATH}" 2>/dev/null; then
        FILES=$(hdfs dfs -ls "${HDFS_PATH}" 2>/dev/null | grep -c "game_player_behavior")
        pass "HDFS 目录已创建: ${HDFS_PATH} (${FILES} 个文件)"
    else
        warn "HDFS 目录尚未创建: ${HDFS_PATH}"
        echo "      Flume HdfsSink 收到数据后会自动创建目录"
    fi
else
    warn "HDFS 不可用，跳过此检查"
fi

# ---- 5. Flume 指标 ----
echo ""
echo "--- [5/5] Flume 运行指标 ---"

if curl -s ${FLUME_METRICS} > /tmp/flume_metrics.json 2>/dev/null; then
    echo "  Source(r1) EventReceivedCount:"
    grep -o '"EventReceivedCount":"[^"]*"' /tmp/flume_metrics.json 2>/dev/null | head -1 || echo "    N/A"
    echo "  Sink(k1) EventDrainSuccessCount:"
    grep -o '"EventDrainSuccessCount":"[^"]*"' /tmp/flume_metrics.json 2>/dev/null | head -1 || echo "    N/A"
    echo "  Sink(k2) EventDrainSuccessCount:"
    grep -o '"EventDrainSuccessCount":"[^"]*"' /tmp/flume_metrics.json 2>/dev/null | tail -1 || echo "    N/A"
else
    warn "Flume 监控不可达 — Flume Agent 可能未启动"
fi

# ---- 完成 ----
echo ""
echo "======================================================"
echo "  链路测试完成"
echo "======================================================"
echo ""
echo "  完整启动流程:"
echo "    1. sh /home/hadoop/data/kafka/start_kafka.sh"
echo "    2. sh /home/hadoop/game_player_anti_addiction/03_flume_config/start_flume.sh --background"
echo "    3. sh /home/hadoop/game_player_anti_addiction/03_flume_config/test_pipeline.sh"
echo ""
