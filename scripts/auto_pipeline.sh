#!/bin/bash
# ============================================================================
# 游戏防沉迷系统 — 每日自动数据管道
# 用法: sh auto_pipeline.sh [日志条数] [生成间隔秒]
# 示例: sh auto_pipeline.sh 3000 0.05    # 每日自动 (默认)
#       sh auto_pipeline.sh 10000 0.1    # 大量数据补录
#
# 管道流程:
#   ① 检查服务状态 → ② 生成模拟日志 → ③ Flume 自动采集到 Kafka/HDFS
#   → ④ 添加 Hive 分区 → ⑤ Spark ETL 清洗+风险标注
#   → ⑥ 同步到 Doris → ⑦ 数据校验 → ⑧ 生成日报
#
# 建议通过 crontab 每日定时执行:
#   0 3 * * * sh /home/hadoop/game_player_anti_addiction/auto_pipeline.sh
# ============================================================================

set -e

export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.412.b08-1.el7_9.x86_64

PROJECT_DIR="/home/hadoop/game_player_anti_addiction"
LOG_DIR="${PROJECT_DIR}/logs"
REPORT_DIR="${PROJECT_DIR}/reports"
PIPELINE_LOG="${LOG_DIR}/pipeline_$(date +%Y%m%d_%H%M%S).log"
TODAY=$(date +%Y%m%d)
RECORDS=2000     # 默认每日更新量
INTERVAL=0.02
MODE="light"     # 默认轻量模式, 传 --heavy 使用 Spark 全链路

# 解析参数
for arg in "$@"; do
    case "$arg" in
        --boot)
            # 开机触发: 更多数据，适合演示
            RECORDS=5000
            MODE="light"
            ;;
        --heavy) MODE="heavy" ;;
        --light) MODE="light" ;;
        --demo)
            # 演示模式: 大量数据
            RECORDS=8000
            MODE="light"
            ;;
        ''|*[!0-9]*) ;;  # 非数字参数跳过
        *) RECORDS="$arg" ;;  # 数字参数作为条数
    esac
done

mkdir -p "${LOG_DIR}" "${REPORT_DIR}" "${PROJECT_DIR}/03_flume_config/logs"

# ============================================================================
# 日志函数
# ============================================================================
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log()    { echo "[$(date '+%H:%M:%S')] $1" | tee -a "${PIPELINE_LOG}"; }
log_ok() { echo -e "[$(date '+%H:%M:%S')] ${GREEN}✓${NC} $1" | tee -a "${PIPELINE_LOG}"; }
log_warn(){ echo -e "[$(date '+%H:%M:%S')] ${YELLOW}⚠${NC} $1" | tee -a "${PIPELINE_LOG}"; }
log_err() { echo -e "[$(date '+%H:%M:%S')] ${RED}✗${NC} $1" | tee -a "${PIPELINE_LOG}"; }

banner() {
    echo "" | tee -a "${PIPELINE_LOG}"
    echo "╔══════════════════════════════════════════════════════════════╗" | tee -a "${PIPELINE_LOG}"
    echo "║  $1" | tee -a "${PIPELINE_LOG}"
    echo "╚══════════════════════════════════════════════════════════════╝" | tee -a "${PIPELINE_LOG}"
}

# ============================================================================
# 辅助函数
# ============================================================================
doris_query() {
    mysql -h 127.0.0.1 -P 9030 -u root --skip-column-names -e "$1" 2>/dev/null
}

doris_query_value() {
    doris_query "$1" | head -1
}

# ============================================================================
# Phase 0 — 日期回填: 检测并补齐缺失日期
# ============================================================================
backfill_missing_dates() {
    banner "Phase 0/⑧  检测缺失日期 & 回填"

    # 查询 Doris 中最新的数据日期
    local last_date=$(doris_query_value \
        "SELECT MAX(dt) FROM game_anti_addiction.dwd_game_player_behavior;")

    if [ -z "${last_date}" ] || [ "${last_date}" = "NULL" ]; then
        log_warn "Doris 中无历史数据，将仅生成今日数据"
        return 0
    fi

    local today=$(date +%Y%m%d)
    log "Doris 最新数据日期: ${last_date}"

    if [ "${last_date}" = "${today}" ]; then
        log_ok "今日数据已存在，跳过回填"
        return 0
    fi

    # 用 Python 计算缺失日期列表 (兼容 Doris 返回的 YYYY-MM-DD 格式)
    local missing_dates=$(python3 -c "
from datetime import datetime, timedelta
last_str = '${last_date}'.replace('-','')  # Doris 可能返回 2026-06-29 或 20260629
today_str = '${today}'
last = datetime.strptime(last_str, '%Y%m%d')
today = datetime.strptime(today_str, '%Y%m%d')
missing = []
d = last + timedelta(days=1)
while d < today:
    missing.append(d.strftime('%Y%m%d'))
    d += timedelta(days=1)
if missing:
    print(' '.join(missing))
" 2>/dev/null)

    if [ -z "${missing_dates}" ]; then
        log "无需回填 (${last_date} → ${today})"
        return 0
    fi

    local missing_count=$(echo "${missing_dates}" | wc -w)
    log_warn "检测到 ${missing_count} 天数据缺失: ${missing_dates}"
    log "开始回填..."

    local backfill_success=0
    local backfill_fail=0

    for current_date in ${missing_dates}; do
        # 检查该日期是否已有数据
        local existing=$(doris_query_value \
            "SELECT COUNT(*) FROM game_anti_addiction.dwd_game_player_behavior WHERE dt='${current_date}';" 2>/dev/null || echo 0)

        if [ "${existing}" -gt 0 ] 2>/dev/null; then
            log "  日期 ${current_date}: 已有 ${existing} 条数据，跳过"
            continue
        fi

        log "  回填日期 ${current_date}..."
        local bf_start=$(date +%s)

        if python3 "${PROJECT_DIR}/lightweight_pipeline.py" 2000 "${current_date}" >> "${PIPELINE_LOG}" 2>&1; then
            local bf_end=$(date +%s)
            local bf_elapsed=$((bf_end - bf_start))
            local bf_rows=$(doris_query_value \
                "SELECT COUNT(*) FROM game_anti_addiction.dwd_game_player_behavior WHERE dt='${current_date}';" 2>/dev/null || echo 0)
            log_ok "  ✓ ${current_date}: 回填 ${bf_rows} 条 (${bf_elapsed}s)"
            backfill_success=$((backfill_success + 1))
        else
            log_err "  ✗ ${current_date}: 回填失败"
            backfill_fail=$((backfill_fail + 1))
        fi
    done

    log "回填完成: 成功 ${backfill_success}, 失败 ${backfill_fail}"

    if [ "${backfill_fail}" -gt 0 ]; then
        log_warn "部分日期回填失败，将继续生成今日数据"
    fi
}

check_process() {
    # $1 = grep pattern, $2 = service name
    if ps aux | grep -q "$1"; then
        return 0
    else
        return 1
    fi
}

# ============================================================================
# Phase ① — 服务健康检查 & 自动拉起
# ============================================================================
check_and_start_services() {
    banner "Phase ①/⑧  服务健康检查"

    local need_flume_restart=false

    # HDFS
    if jps 2>/dev/null | grep -q "NameNode"; then
        log_ok "HDFS NameNode 运行中"
    else
        log_warn "HDFS 未运行，正在启动..."
        /home/hadoop/data/hadoop/sbin/start-dfs.sh >> "${PIPELINE_LOG}" 2>&1
        sleep 8
        jps 2>/dev/null | grep -q "NameNode" && log_ok "HDFS 已启动" || log_err "HDFS 启动失败"
    fi

    # Kafka
    if check_process "[k]afka.Kafka" "Kafka"; then
        log_ok "Kafka Broker 运行中"
    else
        log_warn "Kafka 未运行，正在启动..."
        sh /home/hadoop/data/kafka/start_kafka.sh >> "${PIPELINE_LOG}" 2>&1
        sleep 8
        check_process "[k]afka.Kafka" "Kafka" && log_ok "Kafka 已启动" || log_err "Kafka 启动失败"
        need_flume_restart=true
    fi

    # Flume
    if check_process "[f]lume.node.Application" "Flume"; then
        # 如果 Kafka 重启过，Flume 需要重启以重新连接
        if [ "$need_flume_restart" = true ]; then
            log_warn "Kafka 重启过，重启 Flume 以刷新连接..."
            pkill -f flume.node.Application 2>/dev/null
            sleep 3
        else
            log_ok "Flume Agent 运行中"
        fi
    fi

    if ! check_process "[f]lume.node.Application" "Flume"; then
        log_warn "Flume 未运行，正在启动..."
        nohup /home/hadoop/data/flume/bin/flume-ng agent \
            --name a1 \
            --conf /home/hadoop/data/flume/conf \
            --conf-file "${PROJECT_DIR}/03_flume_config/game_flume.conf" \
            -Dflume.monitoring.type=http \
            -Dflume.monitoring.port=34545 \
            -Duser.timezone=Asia/Shanghai \
            > "${PROJECT_DIR}/03_flume_config/logs/flume_output.log" 2>&1 &
        sleep 8
        check_process "[f]lume.node.Application" "Flume" && log_ok "Flume 已启动" || log_err "Flume 启动失败"
    fi

    # Doris
    if jps 2>/dev/null | grep -q "DorisFE"; then
        log_ok "Doris FE 运行中"
    else
        log_warn "Doris FE 未运行，正在启动..."
        /home/hadoop/data/doris/fe/bin/start_fe.sh --daemon >> "${PIPELINE_LOG}" 2>&1
        sleep 10
        jps 2>/dev/null | grep -q "DorisFE" && log_ok "Doris FE 已启动" || log_err "Doris FE 启动失败"
    fi

    if jps 2>/dev/null | grep -q "DorisBE"; then
        log_ok "Doris BE 运行中"
    else
        log_warn "Doris BE 未运行，正在启动..."
        /home/hadoop/data/doris/be/bin/start_be.sh --daemon >> "${PIPELINE_LOG}" 2>&1
        sleep 8
        jps 2>/dev/null | grep -q "DorisBE" && log_ok "Doris BE 已启动" || log_err "Doris BE 启动失败"
    fi

    # Spring Boot (可选 — 不影响数据管道)
    if check_process "[a]nti-addiction-dashboard" "Spring Boot"; then
        log_ok "Spring Boot 后端运行中"
    else
        log_warn "Spring Boot 未运行，正在启动..."
        local jar_path="${PROJECT_DIR}/target/anti-addiction-dashboard.jar"
        if [ -f "${jar_path}" ]; then
            nohup java -Xmx256m -Xms128m -jar "${jar_path}" \
                --server.port=8081 \
                >> "${LOG_DIR}/backend.log" 2>&1 &
            sleep 5
            check_process "[a]nti-addiction-dashboard" "Spring Boot" && log_ok "Spring Boot 已启动" || log_warn "Spring Boot 启动失败（不影响数据管道）"
        else
            log_warn "JAR 包不存在，请先编译项目"
        fi
    fi
}

# ============================================================================
# Phase ② — 生成今日玩家行为日志
# ============================================================================
generate_logs() {
    banner "Phase ②/⑧  生成模拟玩家行为日志"
    log "参数: ${RECORDS} 条日志, 间隔 ${INTERVAL} 秒/条"

    local start_ts=$(date +%s)
    python3 "${PROJECT_DIR}/generate_game_logs.py" "${RECORDS}" "${INTERVAL}" >> "${PIPELINE_LOG}" 2>&1
    local end_ts=$(date +%s)
    local elapsed=$((end_ts - start_ts))

    # 检查日志文件
    local log_size=$(wc -c < "${PROJECT_DIR}/logs/game_player_behavior.log" 2>/dev/null || echo 0)
    local log_lines=$(wc -l < "${PROJECT_DIR}/logs/game_player_behavior.log" 2>/dev/null || echo 0)

    log_ok "日志生成完成 (耗时 ${elapsed}s, 文件大小 $(numfmt --to=iec ${log_size} 2>/dev/null || echo ${log_size}B), ${log_lines} 行)"
}

# ============================================================================
# Phase ③ — 等待 Flume 采集到 Kafka + HDFS
# ============================================================================
wait_flume_ingestion() {
    banner "Phase ③/⑧  等待 Flume 采集入湖"

    local max_wait=120  # 最多等 2 分钟
    local waited=0
    local check_interval=15

    log "等待 Flume 将日志写入 HDFS..."

    while [ $waited -lt $max_wait ]; do
        sleep $check_interval
        waited=$((waited + check_interval))

        # 检查 Kafka 是否有新消息 (可选验证)
        # 检查 HDFS 目录是否已有今日数据
        local hdfs_count=$(hdfs dfs -ls "/user/flume/game_logs/dt=${TODAY}/" 2>/dev/null | grep -c "FlumeData" || echo 0)
        if [ "${hdfs_count}" -gt 0 ]; then
            log_ok "HDFS 已有 ${hdfs_count} 个 Flume 数据文件 (等待 ${waited}s)"
            return 0
        fi
        log "  等待中... (${waited}s/${max_wait}s) HDFS 尚无今日数据"
    done

    # 超时处理 — 检查日志生成是否正常、Flume 是否健康
    log_warn "HDFS 等待超时 (${max_wait}s)，手动检查..."
    log "  日志文件行数: $(wc -l < ${PROJECT_DIR}/logs/game_player_behavior.log 2>/dev/null || echo 0)"
    log "  Flume 进程: $(ps aux | grep -c '[f]lume.node.Application' || echo 0)"

    # 检查 Flume 输出日志是否有错误
    if [ -f "${PROJECT_DIR}/03_flume_config/logs/flume_output.log" ]; then
        local flume_errors=$(grep -ci "ERROR\|Exception" "${PROJECT_DIR}/03_flume_config/logs/flume_output.log" 2>/dev/null || echo 0)
        log "  Flume 错误数: ${flume_errors}"
        if [ "${flume_errors}" -gt 0 ]; then
            log_warn "Flume 最近错误:"
            grep -i "ERROR\|Exception" "${PROJECT_DIR}/03_flume_config/logs/flume_output.log" 2>/dev/null | tail -3 | tee -a "${PIPELINE_LOG}"
        fi
    fi
}

# ============================================================================
# Phase ④ — 添加 Hive 分区 + MSCK 修复
# ============================================================================
add_hive_partition() {
    banner "Phase ④/⑧  注册 Hive 分区"

    source "${PROJECT_DIR}/env.sh"

    # 先尝试 MSCK REPAIR TABLE 自动发现分区
    log "执行 MSCK REPAIR TABLE..."
    $SPARK_HOME/bin/spark-sql --master local[1] --driver-memory 256m \
        --conf spark.sql.catalogImplementation=hive \
        -e "USE game_anti_addiction; MSCK REPAIR TABLE ods_game_player_behavior;" \
        >> "${PIPELINE_LOG}" 2>&1 || true

    # 确认分区已注册
    local partition_exists=$($SPARK_HOME/bin/spark-sql --master local[1] --driver-memory 256m \
        --conf spark.sql.catalogImplementation=hive \
        --conf spark.sql.adaptive.enabled=false \
        -e "USE game_anti_addiction; SHOW PARTITIONS ods_game_player_behavior;" \
        2>/dev/null | grep -c "${TODAY}" || echo 0)

    if [ "${partition_exists}" -gt 0 ]; then
        log_ok "Hive 分区 dt=${TODAY} 已就绪"
    else
        log_warn "分区未自动发现，尝试手动添加..."
        $SPARK_HOME/bin/spark-sql --master local[1] --driver-memory 256m \
            --conf spark.sql.catalogImplementation=hive \
            -e "USE game_anti_addiction; ALTER TABLE ods_game_player_behavior ADD IF NOT EXISTS PARTITION (dt='${TODAY}') LOCATION '/user/flume/game_logs/dt=${TODAY}';" \
            >> "${PIPELINE_LOG}" 2>&1 || log_warn "手动添加分区失败，ETL 步骤将尝试直接读取"
    fi
}

# ============================================================================
# Phase ⑤ — Spark ETL 清洗 + 风险标注
# ============================================================================
run_spark_etl() {
    banner "Phase ⑤/⑧  Spark ETL 清洗 & 风险标注"

    source "${PROJECT_DIR}/env.sh"

    log "提交 Spark ETL 任务 (local[1], 512m driver)..."
    local etl_start=$(date +%s)

    $SPARK_HOME/bin/spark-submit \
        --master local[1] \
        --driver-memory 512m \
        --conf spark.sql.adaptive.enabled=false \
        --conf spark.sql.adaptive.coalescePartitions.enabled=false \
        --conf spark.sql.shuffle.partitions=2 \
        "${PROJECT_DIR}/spark/etl_anti_addiction.py" "${TODAY}" \
        >> "${PIPELINE_LOG}" 2>&1

    local etl_end=$(date +%s)
    local etl_elapsed=$((etl_end - etl_start))
    log_ok "ETL 完成 (耗时 ${etl_elapsed}s)"
}

# ============================================================================
# Phase ⑥ — 同步 Hive DWD → Doris
# ============================================================================
sync_to_doris() {
    banner "Phase ⑥/⑧  同步数据到 Apache Doris"

    log "通过 Spark SQL 导出 + Stream Load 导入 Doris..."

    sh "${PROJECT_DIR}/doris/doris_manage.sh" sync "${TODAY}" >> "${PIPELINE_LOG}" 2>&1

    # 验证同步结果
    local doris_rows=$(doris_query_value \
        "SELECT COUNT(*) FROM game_anti_addiction.dwd_game_player_behavior WHERE dt='${TODAY}';")

    if [ "${doris_rows}" -gt 0 ] 2>/dev/null; then
        log_ok "Doris 同步成功 (${doris_rows} 行)"
        echo "${doris_rows}" > /tmp/doris_rows_${TODAY}.txt
    else
        log_warn "Doris 今日数据为空，尝试直接 INSERT..."
        # 如果 sync 失败，尝试直接通过 Spark SQL 写入
        log "  备选方案暂未实现，请手动检查"
    fi
}

# ============================================================================
# Phase ⑦ — 数据质量校验
# ============================================================================
verify_data() {
    banner "Phase ⑦/⑧  数据质量校验"

    local d_rows=$(cat /tmp/doris_rows_${TODAY}.txt 2>/dev/null || echo 0)
    local total_players=$(doris_query_value "SELECT COUNT(DISTINCT player_id) FROM game_anti_addiction.dwd_game_player_behavior WHERE dt='${TODAY}';")
    local risk_players=$(doris_query_value "SELECT COUNT(DISTINCT player_id) FROM game_anti_addiction.dwd_game_player_behavior WHERE dt='${TODAY}' AND risk_label>0;")
    local severe=$(doris_query_value "SELECT COUNT(DISTINCT player_id) FROM game_anti_addiction.dwd_game_player_behavior WHERE dt='${TODAY}' AND risk_label=3;")
    local total_recharge=$(doris_query_value "SELECT ROUND(SUM(recharge_amount),2) FROM game_anti_addiction.dwd_game_player_behavior WHERE dt='${TODAY}';")

    log "  总记录数:    ${d_rows}"
    log "  活跃玩家:    ${total_players}"
    log "  风险玩家:    ${risk_players}"
    log "  重度沉迷:    ${severe}"
    log "  总充值金额:  ¥${total_recharge}"

    # 质量检查
    if [ "${d_rows}" -eq 0 ] 2>/dev/null; then
        log_err "今日无数据！请检查管道上游"
        return 1
    fi

    if [ "${total_players}" -eq 0 ] 2>/dev/null; then
        log_warn "今日无活跃玩家"
    fi

    # NULL 值检查
    local null_risk=$(doris_query_value "SELECT COUNT(*) FROM game_anti_addiction.dwd_game_player_behavior WHERE dt='${TODAY}' AND risk_label IS NULL;")
    [ "${null_risk}" != "0" ] && log_warn "risk_label 存在 ${null_risk} 个 NULL" || log_ok "risk_label 字段完整"
}

# ============================================================================
# Phase ⑧ — 生成管道日报
# ============================================================================
generate_daily_report() {
    banner "Phase ⑧/⑧  生成管道日报"

    local report_file="${REPORT_DIR}/pipeline_report_${TODAY}.md"

    cat > "${report_file}" << EOF
# 防沉迷数据管道日报
**日期:** $(date '+%Y-%m-%d %H:%M:%S')
**管道日志:** ${PIPELINE_LOG}

## 数据统计 (Doris)

| 指标 | 数值 |
|------|------|
| DWD 记录数 | $(doris_query_value "SELECT COUNT(*) FROM game_anti_addiction.dwd_game_player_behavior WHERE dt='${TODAY}';") |
| 活跃玩家 | $(doris_query_value "SELECT COUNT(DISTINCT player_id) FROM game_anti_addiction.dwd_game_player_behavior WHERE dt='${TODAY}';") |
| 未成年玩家 | $(doris_query_value "SELECT COUNT(DISTINCT player_id) FROM game_anti_addiction.dwd_game_player_behavior WHERE dt='${TODAY}' AND is_minor=1;") |
| 风险玩家 | $(doris_query_value "SELECT COUNT(DISTINCT player_id) FROM game_anti_addiction.dwd_game_player_behavior WHERE dt='${TODAY}' AND risk_label>0;") |
| 重度沉迷 | $(doris_query_value "SELECT COUNT(DISTINCT player_id) FROM game_anti_addiction.dwd_game_player_behavior WHERE dt='${TODAY}' AND risk_label=3;") |
| 总充值 | ¥$(doris_query_value "SELECT ROUND(SUM(recharge_amount),2) FROM game_anti_addiction.dwd_game_player_behavior WHERE dt='${TODAY}';") |
| 平均在线(min) | $(doris_query_value "SELECT ROUND(AVG(online_duration_min),0) FROM game_anti_addiction.dwd_game_player_behavior WHERE dt='${TODAY}';") |

## 系统资源

- 磁盘使用: $(df -h /home/hadoop/data | tail -1 | awk '{print $5 " used, " $4 " avail"}')
- 内存: $(free -h | awk '/Mem:/ {print $3 "/" $2 " used, " $7 " avail"}')
- 系统负载: $(uptime | awk -F'load average:' '{print $2}')

## Doris 数据量 (最近7天)

\`\`\`
$(doris_query "SELECT dt, COUNT(*) AS rows, COUNT(DISTINCT player_id) AS players FROM game_anti_addiction.dwd_game_player_behavior WHERE dt >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 7 DAY), '%Y%m%d') GROUP BY dt ORDER BY dt DESC;" 2>/dev/null)
\`\`\`

## 管道状态

- HDFS: $(jps 2>/dev/null | grep -q "NameNode" && echo "✅" || echo "❌")
- Kafka: $(ps aux | grep -q "[k]afka.Kafka" && echo "✅" || echo "❌")
- Flume: $(ps aux | grep -q "[f]lume.node.Application" && echo "✅" || echo "❌")
- Doris FE: $(jps 2>/dev/null | grep -q "DorisFE" && echo "✅" || echo "❌")
- Doris BE: $(jps 2>/dev/null | grep -q "DorisBE" && echo "✅" || echo "❌")
- Spring Boot: $(ps aux | grep -q "[a]nti-addiction-dashboard" && echo "✅" || echo "❌")
EOF

    log_ok "日报已生成: ${report_file}"

    # 输出到控制台
    echo ""
    cat "${report_file}"
}

# ============================================================================
# 主流程
# ============================================================================
run_lightweight_pipeline() {
    banner "🚀 轻量级管道 (Python → Stream Load → Doris)"

    log "运行 lightweight_pipeline.py (${RECORDS} 条, 日期 ${TODAY})"
    local lt_start=$(date +%s)

    python3 "${PROJECT_DIR}/lightweight_pipeline.py" "${RECORDS}" "${TODAY}" >> "${PIPELINE_LOG}" 2>&1
    local lt_rc=$?

    local lt_end=$(date +%s)
    local lt_elapsed=$((lt_end - lt_start))

    if [ $lt_rc -eq 0 ]; then
        log_ok "轻量管道完成 (耗时 ${lt_elapsed}s)"
        return 0
    else
        log_err "轻量管道失败 (exit code: $lt_rc)"
        return 1
    fi
}

main() {
    log "╔══════════════════════════════════════════════════════════════╗"
    log "║     游戏防沉迷系统 — 每日自动数据管道                         ║"
    log "║     日期: ${TODAY}  模式: ${MODE}                               ║"
    log "║     条数: ${RECORDS}                                       ║"
    log "╚══════════════════════════════════════════════════════════════╝"

    local pipeline_start=$(date +%s)

    # 检查服务状态 (Doris 是必须的)
    check_and_start_services

    # 日期回填 (检测并补齐缺失日期)
    backfill_missing_dates

    if [ "${MODE}" = "light" ]; then
        # === 轻量模式: Python 直接生成 → Stream Load ===
        log "使用轻量模式 (绕过 Hive/Spark，直接加载 Doris)"
        run_lightweight_pipeline
        verify_data
        generate_daily_report
    else
        # === 完整模式: 全链路 Hadoop 生态 ===
        log "使用完整模式 (Flume → Kafka → HDFS → Hive → Spark → Doris)"

        local existing_rows=$(doris_query_value \
            "SELECT COUNT(*) FROM game_anti_addiction.dwd_game_player_behavior WHERE dt='${TODAY}';" 2>/dev/null || echo 0)
        if [ "${existing_rows}" -gt 0 ] 2>/dev/null; then
            log_warn "Doris 今日已有 ${existing_rows} 条数据"
        fi

        generate_logs
        wait_flume_ingestion
        add_hive_partition
        run_spark_etl
        sync_to_doris
        verify_data
        generate_daily_report
    fi

    local pipeline_end=$(date +%s)
    local total_elapsed=$((pipeline_end - pipeline_start))

    log ""
    log "╔══════════════════════════════════════════════════════════════╗"
    log "║  ✅ 每日管道完成!  总耗时: ${total_elapsed}s ($((total_elapsed / 60))分钟)  ║"
    log "║  大屏现已显示 ${TODAY} 的最新数据                             ║"
    log "╚══════════════════════════════════════════════════════════════╝"

    rm -f /tmp/doris_rows_${TODAY}.txt
}

main "$@"
