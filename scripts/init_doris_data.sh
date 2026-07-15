#!/bin/bash
# ============================================================================
# Doris 测试数据初始化脚本
# 用法: sh init_doris_data.sh
#
# 功能:
#   1. 检查Doris连接状态
#   2. 创建数据库和表 (如不存在)
#   3. 生成7天模拟数据并导入
# ============================================================================
set -e

DORIS_HOST="127.0.0.1"
DORIS_PORT="9030"
DORIS_USER="root"
MYSQL_CMD="mysql -h ${DORIS_HOST} -P ${DORIS_PORT} -u ${DORIS_USER}"

echo "=========================================="
echo "  Doris 测试数据初始化"
echo "=========================================="

# ---- 1. 检查Doris连接 ----
echo "[1/4] 检查Doris连接..."
if ${MYSQL_CMD} -e "SELECT 1 AS ok" 2>/dev/null | grep -q ok; then
    echo "  Doris连接: OK"
else
    echo "  ERROR: 无法连接Doris ${DORIS_HOST}:${DORIS_PORT}"
    echo "  请先启动Doris: sh doris/doris_manage.sh start"
    exit 1
fi

# ---- 2. 创建数据库 ----
echo "[2/4] 创建数据库..."
${MYSQL_CMD} -e "CREATE DATABASE IF NOT EXISTS game_anti_addiction;" 2>/dev/null
echo "  数据库 game_anti_addiction: OK"

# ---- 3. 创建表结构 ----
echo "[3/4] 创建/更新表结构..."
if [ -f doris/doris_init.sql ]; then
    ${MYSQL_CMD} < doris/doris_init.sql 2>/dev/null
    echo "  表结构初始化: OK"
else
    echo "  WARN: doris/doris_init.sql 不存在，跳过建表"
fi

# ---- 4. 生成并导入模拟数据 ----
echo "[4/4] 生成模拟数据并导入Doris..."
echo "  正在生成7天模拟行为数据..."

python3 generate_doris_test_data.py 2>/dev/null | ${MYSQL_CMD} 2>/dev/null &
PID=$!
wait $PID

echo ""
echo "=========================================="
echo "  初始化完成!"
echo "=========================================="

# 显示数据概览
echo ""
echo "=== 数据概览 ==="
${MYSQL_CMD} --table -e "
SELECT
    dt,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT player_id) AS players,
    COUNT(DISTINCT CASE WHEN is_minor=1 THEN player_id END) AS minors,
    COUNT(DISTINCT CASE WHEN risk_label>0 THEN player_id END) AS at_risk
FROM game_anti_addiction.dwd_game_player_behavior
GROUP BY dt ORDER BY dt;
" 2>/dev/null

echo ""
echo "=== 风险分布 ==="
${MYSQL_CMD} --table -e "
SELECT
    risk_label,
    CASE risk_label WHEN 0 THEN '正常' WHEN 1 THEN '预警' WHEN 2 THEN '违规' WHEN 3 THEN '重度沉迷' END AS label,
    COUNT(*) AS cnt
FROM game_anti_addiction.dwd_game_player_behavior
GROUP BY risk_label ORDER BY risk_label;
" 2>/dev/null

echo ""
echo "所有数据集已就绪，可启动SpringBoot后端:"
echo "  java -Xmx256m -Xms128m -jar target/anti-addiction-dashboard.jar"
echo "  然后打开: http://192.168.72.102:8081/big_screen.html"
