#!/bin/bash
# ============================================================================
# 游戏防沉迷系统 — VM 开机自启动脚本
# 用法: sh auto_startup.sh
# 触发: crontab @reboot 或手动执行
#
# 按依赖顺序启动全部服务:
#   HDFS → YARN → Kafka → Flume → Doris FE → Doris BE → Spring Boot
# ============================================================================

set -e

export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.412.b08-1.el7_9.x86_64

PROJECT_DIR="/home/hadoop/game_player_anti_addiction"
LOG_DIR="${PROJECT_DIR}/logs"
STARTUP_LOG="${LOG_DIR}/startup_$(date +%Y%m%d_%H%M%S).log"

mkdir -p "${LOG_DIR}"
mkdir -p "${PROJECT_DIR}/03_flume_config/logs"
mkdir -p "${PROJECT_DIR}/reports"

# ============================================================================
# 日志函数
# ============================================================================
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "${STARTUP_LOG}"
}

log "╔══════════════════════════════════════════════════════════════╗"
log "║     游戏防沉迷系统 — VM 开机自启动                            ║"
log "╚══════════════════════════════════════════════════════════════╝"
log ""

# ============================================================================
# Phase 1: HDFS (先决条件 — Kafka/Flume 依赖)
# ============================================================================
log "[1/6] 启动 Hadoop HDFS..."
if jps 2>/dev/null | grep -q "NameNode"; then
    log "  HDFS NameNode 已在运行，跳过"
else
    /home/hadoop/data/hadoop/sbin/start-dfs.sh >> "${STARTUP_LOG}" 2>&1
    sleep 8
    if jps 2>/dev/null | grep -q "NameNode"; then
        log "  ✓ HDFS 启动成功"
    else
        log "  ✗ HDFS 启动失败，请检查日志"
    fi
fi

# ============================================================================
# Phase 2: YARN (Spark ETL 依赖)
# ============================================================================
log "[2/6] 启动 YARN..."
if jps 2>/dev/null | grep -q "ResourceManager"; then
    log "  YARN ResourceManager 已在运行，跳过"
else
    /home/hadoop/data/hadoop/sbin/start-yarn.sh >> "${STARTUP_LOG}" 2>&1
    sleep 5
    if jps 2>/dev/null | grep -q "ResourceManager"; then
        log "  ✓ YARN 启动成功"
    else
        log "  ✗ YARN 启动失败（可能是内存不足），后续 ETL 将使用 local 模式"
    fi
fi

# ============================================================================
# Phase 3: Kafka (Flume 依赖)
# ============================================================================
log "[3/6] 启动 Kafka..."
if ps aux | grep -q "[k]afka.Kafka"; then
    log "  Kafka 已在运行，跳过"
else
    sh /home/hadoop/data/kafka/start_kafka.sh >> "${STARTUP_LOG}" 2>&1
    sleep 8
    if ps aux | grep -q "[k]afka.Kafka"; then
        log "  ✓ Kafka 启动成功"
    else
        log "  ✗ Kafka 启动失败"
    fi
fi

# ============================================================================
# Phase 4: Flume (日志采集通道)
# ============================================================================
log "[4/6] 启动 Flume Agent..."
if ps aux | grep -q "[f]lume.node.Application"; then
    log "  Flume 已在运行，跳过"
else
    nohup /home/hadoop/data/flume/bin/flume-ng agent \
        --name a1 \
        --conf /home/hadoop/data/flume/conf \
        --conf-file "${PROJECT_DIR}/03_flume_config/game_flume.conf" \
        -Dflume.monitoring.type=http \
        -Dflume.monitoring.port=34545 \
        -Duser.timezone=Asia/Shanghai \
        > "${PROJECT_DIR}/03_flume_config/logs/flume_output.log" 2>&1 &
    sleep 8
    if ps aux | grep -q "[f]lume.node.Application"; then
        log "  ✓ Flume 启动成功"
    else
        log "  ✗ Flume 启动失败，请检查: tail -50 ${PROJECT_DIR}/03_flume_config/logs/flume_output.log"
    fi
fi

# ============================================================================
# Phase 5: Doris (OLAP 引擎 — 大屏数据源)
# ============================================================================
log "[5/6] 启动 Apache Doris..."
DORIS_HOME=/home/hadoop/data/doris

# FE
if jps 2>/dev/null | grep -q "DorisFE"; then
    log "  Doris FE 已在运行，跳过"
else
    ${DORIS_HOME}/fe/bin/start_fe.sh --daemon >> "${STARTUP_LOG}" 2>&1
    sleep 10
    if jps 2>/dev/null | grep -q "DorisFE"; then
        log "  ✓ Doris FE 启动成功"
    else
        log "  ✗ Doris FE 启动失败"
    fi
fi

# BE (需要 FE 先就绪)
if jps 2>/dev/null | grep -q "DorisBE"; then
    log "  Doris BE 已在运行，跳过"
else
    ${DORIS_HOME}/be/bin/start_be.sh --daemon >> "${STARTUP_LOG}" 2>&1
    sleep 8
    if jps 2>/dev/null | grep -q "DorisBE"; then
        log "  ✓ Doris BE 启动成功"
    else
        log "  ✗ Doris BE 启动失败"
    fi
fi

# ============================================================================
# Phase 6: Spring Boot 后端 (大屏 API 服务)
# ============================================================================
log "[6/6] 启动 Spring Boot 后端..."
if ps aux | grep -q "[a]nti-addiction-dashboard"; then
    log "  Spring Boot 已在运行，跳过"
else
    JAR_PATH="${PROJECT_DIR}/target/anti-addiction-dashboard.jar"
    if [ -f "${JAR_PATH}" ]; then
        nohup java -Xmx256m -Xms128m -jar "${JAR_PATH}" \
            --server.port=8081 \
            >> "${LOG_DIR}/backend.log" 2>&1 &
        sleep 5
        if ps aux | grep -q "[a]nti-addiction-dashboard"; then
            log "  ✓ Spring Boot 启动成功 (port 8081)"
        else
            log "  ✗ Spring Boot 启动失败，请检查: tail -50 ${LOG_DIR}/backend.log"
        fi
    else
        log "  ✗ JAR 包不存在: ${JAR_PATH}"
        log "    请先编译: cd ${PROJECT_DIR} && mvn clean package -DskipTests"
    fi
fi

# ============================================================================
# Phase 7: 启动时立即更新今日数据 (后台运行，不阻塞启动)
# ============================================================================
log "[7/7] 触发今日数据更新 (后台)..."
PIPELINE_SCRIPT="${PROJECT_DIR}/auto_pipeline.sh"
PIPELINE_LOG="${LOG_DIR}/boot_pipeline_$(date +%Y%m%d_%H%M%S).log"

if [ -f "${PIPELINE_SCRIPT}" ]; then
    nohup bash "${PIPELINE_SCRIPT}" --boot 5000 >> "${PIPELINE_LOG}" 2>&1 &
    log "  ✓ 数据管道已触发 (PID: $!), 日志: ${PIPELINE_LOG}"
    log "  预计 2-3 分钟后大屏数据更新完成"
else
    log "  ✗ 管道脚本不存在: ${PIPELINE_SCRIPT}"
fi

# ============================================================================
# 启动汇总
# ============================================================================
log ""
log "╔══════════════════════════════════════════════════════════════╗"
log "║  启动完成 — 服务状态汇总                                      ║"
log "╚══════════════════════════════════════════════════════════════╝"
log ""

echo "  === JPS 进程 ===" | tee -a "${STARTUP_LOG}"
jps 2>/dev/null | tee -a "${STARTUP_LOG}"

log ""
echo "  === 服务端口 ===" | tee -a "${STARTUP_LOG}"
echo "  HDFS NameNode:  http://localhost:9870" | tee -a "${STARTUP_LOG}"
echo "  YARN Resource:  http://localhost:8088" | tee -a "${STARTUP_LOG}"
echo "  Kafka:          localhost:9092" | tee -a "${STARTUP_LOG}"
echo "  Flume Monitor:  http://localhost:34545" | tee -a "${STARTUP_LOG}"
echo "  Doris FE Web:   http://localhost:8030" | tee -a "${STARTUP_LOG}"
echo "  Doris BE Web:   http://localhost:8040" | tee -a "${STARTUP_LOG}"
echo "  Doris Query:    localhost:9030" | tee -a "${STARTUP_LOG}"
echo "  Dashboard API:  http://localhost:8081" | tee -a "${STARTUP_LOG}"
echo "  Dashboard UI:   http://localhost:8081" | tee -a "${STARTUP_LOG}"

log ""
log "  启动日志: ${STARTUP_LOG}"
log "  后端日志: tail -f ${LOG_DIR}/backend.log"
log "  Flume日志: tail -f ${PROJECT_DIR}/03_flume_config/logs/flume_output.log"
