#!/bin/bash
# ============================================================================
# 游戏平台防沉迷系统 — 自动化数据验证与监控脚本
# 用法: sh verify_monitor.sh {verify|monitor|full|schedule}
# ============================================================================
# 功能:
#   verify   — 一次性数据一致性校验 (Doris ↔ Hive 行数对比, 数据质量检查)
#   monitor  — 实时监控 Doris BE 状态 + 查询性能
#   full     — 全链路健康检查 (HDFS → Kafka → Hive → Doris)
#   schedule — 启动定时监控 daemon (每5分钟检查一次)
# ============================================================================

export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.412.b08-1.el7_9.x86_64
DORIS_HOST="127.0.0.1"
DORIS_PORT="9030"
DORIS_USER="root"
DORIS_DB="game_anti_addiction"
REPORT_DIR="/home/hadoop/game_player_anti_addiction/reports"
ALERT_LOG="${REPORT_DIR}/alerts.log"
LOG_FILE="${REPORT_DIR}/verify_$(date +%Y%m%d_%H%M%S).log"

mkdir -p "${REPORT_DIR}"

# ============================================================================
# 颜色输出
# ============================================================================
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log()  { echo -e "[$(date '+%H:%M:%S')] $1" | tee -a "${LOG_FILE}"; }
pass() { echo -e "${GREEN}[PASS]${NC} $1" | tee -a "${LOG_FILE}"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1" | tee -a "${LOG_FILE}"; }
fail() { echo -e "${RED}[FAIL]${NC} $1" | tee -a "${LOG_FILE}"; }

alert() {
    local msg="[$(date '+%Y-%m-%d %H:%M:%S')] ALERT: $1"
    echo "${msg}" >> "${ALERT_LOG}"
    echo -e "${RED}${msg}${NC}" | tee -a "${LOG_FILE}"
}

# ============================================================================
# 通用函数
# ============================================================================
doris_query() {
    mysql -h "${DORIS_HOST}" -P "${DORIS_PORT}" -u "${DORIS_USER}" --skip-column-names -e "$1" 2>/dev/null
}

doris_query_value() {
    doris_query "$1" | head -1
}

# ============================================================================
# 1. Doris 服务状态检查
# ============================================================================
check_doris_status() {
    log ">>> 检查 Doris 服务状态 <<<"
    local ok=0

    # FE 状态
    if ps aux | grep -q "[D]orisFE"; then
        local fe_pid=$(ps aux | grep "[D]orisFE" | awk '{print $2}')
        pass "Doris FE 运行中 (PID: ${fe_pid})"
    else
        fail "Doris FE 未运行!"
        alert "Doris FE is DOWN"
        ok=1
    fi

    # BE 状态 — 通过 SHOW PROC '/backends' 解析 (兼容 Doris 2.0.x)
    local be_info=$(doris_query "SHOW PROC '/backends';" 2>/dev/null | grep "192.168.72.102")
    local be_alive=$(echo "${be_info}" | awk -F'\t' '{print $9}')
    local be_data_used=$(echo "${be_info}" | awk -F'\t' '{print $12}')
    local be_avail=$(echo "${be_info}" | awk -F'\t' '{print $14}')
    local be_total=$(echo "${be_info}" | awk -F'\t' '{print $15}')
    local be_used_pct=$(echo "${be_info}" | awk -F'\t' '{print $16}')

    if [ "${be_alive}" = "true" ]; then
        local be_pid=$(ps aux | grep "[d]oris_be" | awk '{print $2}')
        pass "Doris BE 运行中 (PID: ${be_pid})"
    else
        fail "Doris BE 未运行或不健康!"
        alert "Doris BE is DOWN or unhealthy"
        ok=1
    fi

    log "  BE 指标: Alive=${be_alive}, DataUsed=${be_data_used}, Avail=${be_avail}, Total=${be_total}, UsedPct=${be_used_pct}"

    return ${ok}
}

# ============================================================================
# 2. 数据一致性校验 (Doris 内部校验)
# ============================================================================
verify_data_consistency() {
    local dt="${1:-$(date +%Y%m%d)}"
    log ">>> 数据一致性校验 (日期: ${dt}) <<<"

    # 2.1 分区行数
    local row_count=$(doris_query_value "SELECT COUNT(*) FROM ${DORIS_DB}.dwd_game_player_behavior WHERE dt='${dt}';")
    log "  DWD 表分区 ${dt} 行数: ${row_count}"

    if [ "${row_count}" -eq 0 ] 2>/dev/null; then
        warn "  ${dt} 分区无数据，请运行 ETL 任务加载数据"
        return 1
    elif [ "${row_count}" -gt 0 ] 2>/dev/null; then
        pass "  ${dt} 分区数据存在 (${row_count} 行)"
    fi

    # 2.2 NULL 值检查 (关键防沉迷字段)
    local null_risk=$(doris_query_value "SELECT COUNT(*) FROM ${DORIS_DB}.dwd_game_player_behavior WHERE dt='${dt}' AND risk_label IS NULL;")
    local null_age=$(doris_query_value "SELECT COUNT(*) FROM ${DORIS_DB}.dwd_game_player_behavior WHERE dt='${dt}' AND is_minor IS NULL;")
    local null_duration=$(doris_query_value "SELECT COUNT(*) FROM ${DORIS_DB}.dwd_game_player_behavior WHERE dt='${dt}' AND online_duration_min IS NULL;")

    [ "${null_risk}" != "0" ] && warn "risk_label 存在 ${null_risk} 个 NULL 值" || pass "risk_label 无 NULL 值"
    [ "${null_age}" != "0" ] && warn "is_minor 存在 ${null_age} 个 NULL 值" || pass "is_minor 无 NULL 值"
    [ "${null_duration}" != "0" ] && warn "online_duration_min 存在 ${null_duration} 个 NULL 值" || pass "online_duration_min 无 NULL 值"

    # 2.3 数据质量指标
    local total_players=$(doris_query_value "SELECT COUNT(DISTINCT player_id) FROM ${DORIS_DB}.dwd_game_player_behavior WHERE dt='${dt}';")
    local risk_count=$(doris_query_value "SELECT COUNT(DISTINCT player_id) FROM ${DORIS_DB}.dwd_game_player_behavior WHERE dt='${dt}' AND risk_label>0;")
    local minor_count=$(doris_query_value "SELECT COUNT(DISTINCT player_id) FROM ${DORIS_DB}.dwd_game_player_behavior WHERE dt='${dt}' AND is_minor=1;")

    log "  总玩家: ${total_players}, 风险玩家: ${risk_count}, 未成年: ${minor_count}"

    # 2.4 充值金额合理性检查
    local neg_recharge=$(doris_query_value "SELECT COUNT(*) FROM ${DORIS_DB}.dwd_game_player_behavior WHERE dt='${dt}' AND recharge_amount < 0;")
    local huge_recharge=$(doris_query_value "SELECT COUNT(*) FROM ${DORIS_DB}.dwd_game_player_behavior WHERE dt='${dt}' AND recharge_amount > 50000;")
    [ "${neg_recharge}" != "0" ] && warn "存在 ${neg_recharge} 笔负充值金额 (异常)" || pass "充值金额无负值"
    [ "${huge_recharge}" != "0" ] && warn "存在 ${huge_recharge} 笔超大额充值 (>50000)" || pass "充值金额在合理范围"

    # 2.5 时间字段合理性
    local future_dt=$(doris_query_value "SELECT COUNT(*) FROM ${DORIS_DB}.dwd_game_player_behavior WHERE dt > DATE_FORMAT(NOW(), '%Y%m%d');")
    local zero_duration=$(doris_query_value "SELECT COUNT(*) FROM ${DORIS_DB}.dwd_game_player_behavior WHERE dt='${dt}' AND online_duration <= 0;")
    [ "${future_dt}" != "0" ] && warn "存在 ${future_dt} 条未来日期记录" || pass "无未来日期数据"
    [ "${zero_duration}" != "0" ] && warn "存在 ${zero_duration} 条在线时长为0的记录" || pass "在线时长数据正常"
}

# ============================================================================
# 3. 全链路健康检查
# ============================================================================
check_pipeline_health() {
    log ">>> 全链路健康检查 <<<"

    # HDFS
    if ps aux | grep -q "[N]ameNode"; then
        pass "HDFS NameNode 运行中"
    else
        warn "HDFS NameNode 未运行"
    fi

    # Kafka
    if ps aux | grep -q "[k]afka.Kafka"; then
        pass "Kafka Broker 运行中"
    else
        warn "Kafka Broker 未运行"
    fi

    # Doris FE
    if ps aux | grep -q "[D]orisFE"; then
        pass "Doris FE 运行中"
    else
        fail "Doris FE 未运行!"
        alert "Doris FE is DOWN"
    fi

    # Doris BE
    local be_alive=$(doris_query "SHOW PROC '/backends';" 2>/dev/null | grep "192.168.72.102" | awk -F'\t' '{print $9}')
    [ "${be_alive}" = "true" ] && pass "Doris BE Alive" || { fail "Doris BE 不可用!"; alert "Doris BE is not Alive"; }

    # 磁盘空间
    local disk_pct=$(df /home/hadoop/data | tail -1 | awk '{print $5}' | tr -d '%')
    if [ "${disk_pct}" -gt 85 ]; then
        warn "数据盘使用率: ${disk_pct}% (建议清理)"
    else
        pass "数据盘使用率: ${disk_pct}%"
    fi

    # 内存
    local mem_avail=$(free -m | awk '/Mem:/ {print $7}')
    if [ "${mem_avail}" -lt 500 ]; then
        warn "可用内存: ${mem_avail}MB (偏低，可能影响查询)"
    else
        pass "可用内存: ${mem_avail}MB"
    fi
}

# ============================================================================
# 4. 查询性能基准测试
# ============================================================================
run_query_benchmark() {
    log ">>> 查询性能基准测试 <<<"

    local queries=(
        "KPI指标:SELECT COUNT(DISTINCT player_id) FROM ${DORIS_DB}.dwd_game_player_behavior WHERE dt='$(date +%Y%m%d)';"
        "时长分布:SELECT COUNT(*) FROM ${DORIS_DB}.dwd_game_player_behavior WHERE dt='$(date +%Y%m%d)' GROUP BY CASE WHEN online_duration_min<60 THEN 'short' ELSE 'long' END;"
        "风险统计:SELECT risk_label,COUNT(*) FROM ${DORIS_DB}.dwd_game_player_behavior WHERE dt='$(date +%Y%m%d)' GROUP BY risk_label;"
    )

    for q in "${queries[@]}"; do
        local name="${q%%:*}"
        local sql="${q#*:}"
        local start=$(date +%s%N)
        doris_query "${sql}" > /dev/null 2>&1
        local end=$(date +%s%N)
        local ms=$(( (end - start) / 1000000 ))
        if [ "${ms}" -lt 1000 ]; then
            pass "${name}: ${ms}ms"
        elif [ "${ms}" -lt 5000 ]; then
            warn "${name}: ${ms}ms (偏慢)"
        else
            fail "${name}: ${ms}ms (超时风险!)"
        fi
    done
}

# ============================================================================
# 5. 生成校验报告
# ============================================================================
generate_report() {
    local report_file="${REPORT_DIR}/health_report_$(date +%Y%m%d_%H%M%S).md"

    cat > "${report_file}" << EOF
# 游戏防沉迷系统 - 数据健康报告
**生成时间:** $(date '+%Y-%m-%d %H:%M:%S')

## Doris 服务状态
- FE: $(ps aux | grep -q "[D]orisFE" && echo "✅ 运行中" || echo "❌ 未运行")
- BE: $(be_alive=$(doris_query "SHOW PROC '/backends';" 2>/dev/null | grep "192.168.72.102" | awk -F'\t' '{print $9}'); [ "${be_alive}" = "true" ] && echo '✅ Alive' || echo '❌ Dead')

## 数据统计 (今日)
- DWD 总行数: $(doris_query_value "SELECT COUNT(*) FROM ${DORIS_DB}.dwd_game_player_behavior WHERE dt='$(date +%Y%m%d)';")
- 活跃玩家: $(doris_query_value "SELECT COUNT(DISTINCT player_id) FROM ${DORIS_DB}.dwd_game_player_behavior WHERE dt='$(date +%Y%m%d)';")
- 风险玩家: $(doris_query_value "SELECT COUNT(DISTINCT player_id) FROM ${DORIS_DB}.dwd_game_player_behavior WHERE dt='$(date +%Y%m%d)' AND risk_label>0;")
- 重度沉迷: $(doris_query_value "SELECT COUNT(DISTINCT player_id) FROM ${DORIS_DB}.dwd_game_player_behavior WHERE dt='$(date +%Y%m%d)' AND risk_label=3;")
- 总充值: ¥$(doris_query_value "SELECT ROUND(SUM(recharge_amount),2) FROM ${DORIS_DB}.dwd_game_player_behavior WHERE dt='$(date +%Y%m%d)';")

## 系统资源
- 磁盘: $(df -h /home/hadoop/data | tail -1 | awk '{print $5 " used, " $4 " avail"}')
- 内存: $(free -h | awk '/Mem:/ {print $3 "/" $2 " used, " $7 " avail"}')
- 系统负载: $(uptime | awk -F'load average:' '{print $2}')

## 最近告警
\`\`\`
$(tail -20 "${ALERT_LOG}" 2>/dev/null || echo "无告警")
\`\`\`
EOF

    log "报告已生成: ${report_file}"
    echo "${report_file}"
}

# ============================================================================
# 6. 定时监控 Daemon
# ============================================================================
run_schedule() {
    log ">>> 启动定时监控 (每5分钟) <<<"
    log "PID: $$, 日志: ${REPORT_DIR}/monitor_daemon.log"

    local iteration=0
    while true; do
        iteration=$((iteration + 1))
        echo "" >> "${REPORT_DIR}/monitor_daemon.log"
        echo "=== Iteration ${iteration} @ $(date) ===" >> "${REPORT_DIR}/monitor_daemon.log"

        # 核心检查 (轻量级)
        local be_alive=$(doris_query "SHOW PROC '/backends';" 2>/dev/null | grep "192.168.72.102" | awk -F'\t' '{print $9}')
        if [ "${be_alive}" != "true" ]; then
            alert "Iteration ${iteration}: Doris BE is NOT Alive!"
        fi

        local fe_running=$(ps aux | grep -c "[D]orisFE")
        if [ "${fe_running}" -eq 0 ]; then
            alert "Iteration ${iteration}: Doris FE is NOT running!"
        fi

        # 磁盘检查
        local disk_pct=$(df /home/hadoop/data | tail -1 | awk '{print $5}' | tr -d '%')
        if [ "${disk_pct}" -gt 90 ]; then
            alert "Iteration ${iteration}: Disk usage ${disk_pct}% exceeds 90%!"
        fi

        # 每30分钟做一次全量检查
        if [ $((iteration % 6)) -eq 0 ]; then
            local row_count=$(doris_query_value "SELECT COUNT(*) FROM ${DORIS_DB}.dwd_game_player_behavior WHERE dt='$(date +%Y%m%d)';" 2>/dev/null)
            echo "  Full check: DWD rows today=${row_count}, disk=${disk_pct}%, mem_avail=$(free -m | awk '/Mem:/{print $7}')MB" >> "${REPORT_DIR}/monitor_daemon.log"
        fi

        sleep 300  # 5分钟间隔
    done
}

# ============================================================================
# 主入口
# ============================================================================
case "${1}" in
    verify)
        echo "=========================================="
        echo "  数据一致性校验"
        echo "=========================================="
        check_doris_status
        verify_data_consistency "${2:-$(date +%Y%m%d)}"
        ;;

    monitor)
        echo "=========================================="
        echo "  Doris 状态监控"
        echo "=========================================="
        check_doris_status
        run_query_benchmark
        ;;

    full)
        echo "=========================================="
        echo "  全链路健康检查"
        echo "=========================================="
        check_pipeline_health
        check_doris_status
        verify_data_consistency "${2:-$(date +%Y%m%d)}"
        run_query_benchmark
        REPORT=$(generate_report)
        echo ""
        echo "=========================================="
        log "全链路检查完成"
        log "报告: ${REPORT}"
        echo "=========================================="
        ;;

    schedule)
        echo "=========================================="
        echo "  启动定时监控 Daemon"
        echo "=========================================="
        nohup bash "$0" _daemon_impl >> "${REPORT_DIR}/monitor_daemon.log" 2>&1 &
        DAEMON_PID=$!
        echo "Daemon PID: ${DAEMON_PID}"
        echo "停止: kill ${DAEMON_PID}"
        echo "日志: tail -f ${REPORT_DIR}/monitor_daemon.log"
        ;;

    _daemon_impl)
        run_schedule
        ;;

    *)
        echo "用法: sh verify_monitor.sh {verify|monitor|full|schedule} [date]"
        echo ""
        echo "  verify [dt]  — 数据一致性校验 (默认今天)"
        echo "  monitor      — Doris BE 状态 + 查询性能基准测试"
        echo "  full [dt]    — 全链路健康检查 + 生成报告"
        echo "  schedule     — 启动后台定时监控 (每5分钟)"
        echo ""
        echo "示例:"
        echo "  sh verify_monitor.sh verify 20260610"
        echo "  sh verify_monitor.sh full"
        echo "  sh verify_monitor.sh schedule"
        echo "  tail -f reports/monitor_daemon.log"
        ;;
esac
