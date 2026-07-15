#!/bin/bash
# ============================================================================
# 游戏平台玩家行为分析与防沉迷系统 — 一键启动脚本
# ============================================================================
# 用法: sh start_all.sh [日志条数] [生成间隔秒]
# 示例: sh start_all.sh 10000 0.5
#       sh start_all.sh 50000 0   (快速生成5万条)
# ============================================================================

set -e

PROJECT_DIR=$(cd "$(dirname "$0")" && pwd)
RECORDS=${1:-10000}
INTERVAL=${2:-0.5}
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║     游戏平台玩家行为分析与防沉迷系统 v1.0                      ║"
echo "║     Hadoop + Flume + Kafka + Hive + Spark + Doris + FineBI   ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""
echo "  项目路径: ${PROJECT_DIR}"
echo "  日志条数: ${RECORDS}"
echo "  生成间隔: ${INTERVAL}秒/条"
echo "  启动时间: ${TIMESTAMP}"
echo ""

# ------------------------------------------------------------------
# Phase 1: Python 日志生成 (本地直接运行)
# ------------------------------------------------------------------
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  Phase 1/5: 生成模拟玩家行为日志                              ║"
echo "╚══════════════════════════════════════════════════════════════╝"

python3 "${PROJECT_DIR}/generate_game_logs.py" ${RECORDS} ${INTERVAL}

echo ""
echo "  ✓ Phase 1 完成: 日志文件已生成"
echo "    路径: ${PROJECT_DIR}/logs/game_player_behavior.log"

# ------------------------------------------------------------------
# Phase 2: Flume 启动 (需Hadoop+Kafka集群)
# ------------------------------------------------------------------
echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  Phase 2/5: 启动 Flume Agent                                 ║"
echo "╚══════════════════════════════════════════════════════════════╝"

FLUME_HOME=${FLUME_HOME:-/usr/local/flume}
if [ -f "${FLUME_HOME}/bin/flume-ng" ]; then
    echo "  Flume 已检测到: ${FLUME_HOME}"
    echo "  启动命令:"
    echo "    nohup ${FLUME_HOME}/bin/flume-ng agent \\"
    echo "      --name a1 \\"
    echo "      --conf ${FLUME_HOME}/conf \\"
    echo "      --conf-file ${PROJECT_DIR}/flume/flume.conf &"
    echo ""
    echo "  [跳过] 如需自动启动，取消下面注释:"
    echo "  # nohup ${FLUME_HOME}/bin/flume-ng agent --name a1 --conf ${FLUME_HOME}/conf --conf-file ${PROJECT_DIR}/flume/flume.conf > ${PROJECT_DIR}/flume/flume_output.log 2>&1 &"
else
    echo "  [跳过] Flume 未安装或 FLUME_HOME 未设置"
    echo "  安装Flume后执行:"
    echo "    sh ${PROJECT_DIR}/flume/start_flume.sh"
fi

# ------------------------------------------------------------------
# Phase 3: Hive DDL (需Hive集群)
# ------------------------------------------------------------------
echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  Phase 3/5: Hive 建表 (ODS → DWD → ADS)                      ║"
echo "╚══════════════════════════════════════════════════════════════╝"

HIVE_CLI=${HIVE_CLI:-beeline}
echo "  执行方式:"
echo "    ${HIVE_CLI} -u \"jdbc:hive2://<hiveserver2>:10000\" -f ${PROJECT_DIR}/hive/hive_ddl.sql"
echo ""
echo "  [跳过] 需 HiveServer2 运行中，手动执行上方命令"

# ------------------------------------------------------------------
# Phase 4: Spark ETL (需YARN集群)
# ------------------------------------------------------------------
echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  Phase 4/5: Spark ETL 清洗+风险标注                           ║"
echo "╚══════════════════════════════════════════════════════════════╝"

TODAY=$(date +%Y%m%d)
echo "  提交命令 (YARN Client):"
echo "    spark-submit \\"
echo "      --master yarn \\"
echo "      --deploy-mode client \\"
echo "      --num-executors 5 \\"
echo "      --executor-memory 4G \\"
echo "      --executor-cores 2 \\"
echo "      --driver-memory 2G \\"
echo "      ${PROJECT_DIR}/spark/etl_anti_addiction.py ${TODAY}"
echo ""
echo "  提交命令 (YARN Cluster - 生产推荐):"
echo "    spark-submit \\"
echo "      --master yarn \\"
echo "      --deploy-mode cluster \\"
echo "      ${PROJECT_DIR}/spark/etl_anti_addiction.py ${TODAY}"
echo ""
echo "  [跳过] 需 YARN 集群运行中，手动执行上方命令"

# ------------------------------------------------------------------
# Phase 5: Doris 同步 (需Doris集群)
# ------------------------------------------------------------------
echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  Phase 5/5: Doris 建表 + 数据同步                             ║"
echo "╚══════════════════════════════════════════════════════════════╝"

echo "  同步步骤:"
echo "    1. MySQL客户端连接Doris FE: mysql -h <doris-fe> -P 9030 -u root"
echo "    2. 执行建表+同步: source ${PROJECT_DIR}/doris/doris_ddl_sync_query.sql"
echo "    3. FineBI连接Doris建可视化大屏 (参考 finebi/DASHBOARD_DESIGN.md)"
echo ""
echo "  [跳过] 需 Doris 集群运行中，手动执行上方命令"

# ------------------------------------------------------------------
# 完成
# ------------------------------------------------------------------
echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  项目搭建完成!                                                ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""
echo "  项目文件结构:"
echo ""
find "${PROJECT_DIR}" -type f -not -path "*/logs/*" -not -name "*.pyc" | sort | while read f; do
    size=$(wc -c < "$f")
    lines=$(wc -l < "$f")
    printf "    %-55s %6d行 %8d字节\n" "${f#$PROJECT_DIR/}" "$lines" "$size"
done

echo ""
echo "  已生成日志:"
ls -lh "${PROJECT_DIR}/logs/"*.log 2>/dev/null || echo "    (无)"
echo ""
echo "  本地可运行组件:"
echo "    ✓ Python日志生成器 — 已验证通过"
echo "  需集群组件 (按顺序部署):"
echo "    ○ Flume  → 需 Hadoop + Kafka 集群"
echo "    ○ Hive   → 需 HiveServer2 运行"
echo "    ○ Spark  → 需 YARN 集群"
echo "    ○ Doris  → 需 Doris FE/BE 集群"
echo "    ○ FineBI → 需 FineBI 服务 + Doris 连接"
echo ""
echo "  下一步: 依次启动集群各组件，按 Phase 2→5 执行"
