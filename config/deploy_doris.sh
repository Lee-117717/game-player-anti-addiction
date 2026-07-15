#!/bin/bash
# ============================================================================
# Apache Doris 2.0.14 单节点部署脚本 (FE + BE)
# 适配 CentOS 虚拟机低资源环境
# 用法: sh deploy_doris.sh
# ============================================================================
set -e

JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.412.b08-1.el7_9.x86_64}
DORIS_TARBALL=/home/hadoop/data/apache-doris-2.0.14-bin-x64.tar.gz
DORIS_HOME=/home/hadoop/data/doris
LOG_BASE=/home/hadoop/data/logs/doris
DATA_BASE=/home/hadoop/data/doris

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo "=============================================="
echo "  Apache Doris 2.0.14 单节点部署"
echo "  安装路径: ${DORIS_HOME}"
echo "  日志路径: ${LOG_BASE}"
echo "=============================================="

# ---- Step 1: Extract ----
echo ""
echo "[1/6] 解压安装包..."
if [ ! -f "${DORIS_TARBALL}" ]; then
    echo -e "${RED}[ERROR]${NC} 安装包不存在: ${DORIS_TARBALL}"
    exit 1
fi

# Remove old doris directory if empty/partial
if [ -d "${DORIS_HOME}" ]; then
    if [ -d "${DORIS_HOME}/fe" ] || [ -d "${DORIS_HOME}/be" ]; then
        echo "  检测到已有 Doris 目录，跳过解压"
    else
        rm -rf "${DORIS_HOME}"
        tar -xzf "${DORIS_TARBALL}" -C /home/hadoop/data/
        # Rename extracted directory to 'doris'
        EXTRACTED_DIR=$(ls -d /home/hadoop/data/apache-doris-2.0.*-bin-x64 2>/dev/null | head -1)
        if [ -n "${EXTRACTED_DIR}" ] && [ "${EXTRACTED_DIR}" != "${DORIS_HOME}" ]; then
            mv "${EXTRACTED_DIR}" "${DORIS_HOME}"
            echo "  重命名: $(basename ${EXTRACTED_DIR}) -> doris"
        fi
    fi
else
    tar -xzf "${DORIS_TARBALL}" -C /home/hadoop/data/
    EXTRACTED_DIR=$(ls -d /home/hadoop/data/apache-doris-2.0.*-bin-x64 2>/dev/null | head -1)
    if [ -n "${EXTRACTED_DIR}" ] && [ "${EXTRACTED_DIR}" != "${DORIS_HOME}" ]; then
        mv "${EXTRACTED_DIR}" "${DORIS_HOME}"
        echo "  重命名: $(basename ${EXTRACTED_DIR}) -> doris"
    fi
fi
echo -e "${GREEN}[OK]${NC} 解压完成"

# ---- Step 2: Configure FE ----
echo ""
echo "[2/6] 配置 FE (Frontend)..."
FE_CONF=${DORIS_HOME}/fe/conf/fe.conf
cp ${FE_CONF} ${FE_CONF}.bak 2>/dev/null || true

cat > ${FE_CONF} << 'FECONF'
# ============================================================================
# Apache Doris 2.0.14 FE 配置 — 单节点低资源模式
# ============================================================================

# Java 环境
JAVA_HOME = /usr/lib/jvm/java-1.8.0-openjdk-1.8.0.412.b08-1.el7_9.x86_64

# JVM 参数：低内存模式 (256m-512m)
JAVA_OPTS = -Xms256m -Xmx512m -XX:+UseSerialGC -XX:MaxGCPauseMillis=500 -Dfile.encoding=UTF-8
JAVA_OPTS_FOR_JDK_9 = -Xms256m -Xmx512m -XX:+UseSerialGC -Dfile.encoding=UTF-8

# 日志目录 -> 写入数据盘，避免系统盘满
LOG_DIR = /home/hadoop/data/logs/doris/fe

# 元数据存储目录
meta_dir = /home/hadoop/data/doris/fe/doris-meta

# 端口配置
http_port = 8030
rpc_port = 9020
query_port = 9030
edit_log_port = 9010

# 单节点模式
metadata_failure_recovery = false

# 心跳超时
heartbeat_timeout_second = 120

# 低资源调优
qe_max_connection = 100
max_running_txn_num_per_db = 100

# 单副本 (生产环境需 >=3)
default_replication_num = 1

# 关闭审计日志减少 IO
enable_audit_plugin = false
FECONF
echo -e "${GREEN}[OK]${NC} FE 配置完成"

# ---- Step 3: Configure BE ----
echo ""
echo "[3/6] 配置 BE (Backend)..."
BE_CONF=${DORIS_HOME}/be/conf/be.conf
cp ${BE_CONF} ${BE_CONF}.bak 2>/dev/null || true

cat > ${BE_CONF} << 'BECONF'
# ============================================================================
# Apache Doris 2.0.14 BE 配置 — 单节点低资源模式
# ============================================================================

# Java 环境
JAVA_HOME = /usr/lib/jvm/java-1.8.0-openjdk-1.8.0.412.b08-1.el7_9.x86_64

# JVM 参数
JAVA_OPTS = -Xms128m -Xmx256m -XX:+UseSerialGC -Dfile.encoding=UTF-8

# 日志目录 -> 写入数据盘
LOG_DIR = /home/hadoop/data/logs/doris/be

# 数据存储目录 (可多个，分号分隔)
storage_root_path = /home/hadoop/data/doris/be/storage

# BE 端口
be_port = 9060
webserver_port = 8040
heartbeat_service_port = 9050
brpc_port = 8060

# 内存限制：物理内存的 50% (5.3G * 0.5 ≈ 2.6G)
mem_limit = 50%

# 单副本
default_replication_num = 1

# 低资源调优
buffer_pool_limit = 20%
max_tablet_version_num = 500

# 关闭审计日志
enable_audit_plugin = false
BECONF

# Create storage directory
mkdir -p /home/hadoop/data/doris/be/storage
echo -e "${GREEN}[OK]${NC} BE 配置完成"

# ---- Step 4: Initialize FE (first time only) ----
echo ""
echo "[4/6] 初始化 FE 元数据..."
if [ ! -f "${DORIS_HOME}/fe/doris-meta/image/ROLE" ]; then
    ${DORIS_HOME}/fe/bin/start_fe.sh --daemon 2>&1 || true
    sleep 5
    # Check if FE started
    if ${DORIS_HOME}/fe/bin/stop_fe.sh 2>/dev/null; then
        echo "  FE 元数据初始化完成"
    fi
    sleep 2
else
    echo "  FE 元数据已存在，跳过初始化"
fi
echo -e "${GREEN}[OK]${NC} FE 初始化完成"

# ---- Step 5: Add BE to cluster ----
echo ""
echo "[5/6] 注册 BE 节点..."
# This is done after FE is fully started

# ---- Step 6: Create management scripts ----
echo ""
echo "[6/6] 生成管理脚本..."

# start_doris.sh
cat > ${DORIS_HOME}/start_doris.sh << 'STARTSCRIPT'
#!/bin/bash
# ============================================================================
# 启动 Apache Doris (先 FE，后 BE)
# ============================================================================
JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.412.b08-1.el7_9.x86_64}
DORIS_HOME=/home/hadoop/data/doris

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

echo "=== Starting Apache Doris ==="

# ---- Start FE ----
echo -n "[FE] Starting... "
if ps aux | grep -q "[D]orisFE"; then
    echo -e "${GREEN}Already Running${NC}"
else
    nohup ${DORIS_HOME}/fe/bin/start_fe.sh --daemon > /home/hadoop/data/logs/doris/fe_startup.log 2>&1 &
    sleep 3
    if ps aux | grep -q "[D]orisFE"; then
        echo -e "${GREEN}OK${NC} (PID: $(jps | grep DorisFE | awk '{print $1}'))"
    else
        echo -e "${RED}FAIL${NC} — check log: /home/hadoop/data/logs/doris/fe/fe.log"
        exit 1
    fi
fi

# Wait for FE to be ready
echo -n "[FE] Waiting for readiness... "
for i in $(seq 1 30); do
    if curl -s http://127.0.0.1:8030/api/bootstrap > /dev/null 2>&1; then
        echo -e "${GREEN}Ready${NC}"
        break
    fi
    sleep 2
    if [ $i -eq 30 ]; then
        echo -e "${RED}Timeout${NC}"
        exit 1
    fi
done

# ---- Start BE ----
echo -n "[BE] Starting... "
if ps aux | grep -q "[D]orisBE"; then
    echo -e "${GREEN}Already Running${NC}"
else
    nohup ${DORIS_HOME}/be/bin/start_be.sh --daemon > /home/hadoop/data/logs/doris/be_startup.log 2>&1 &
    sleep 3
    if ps aux | grep -q "[D]orisBE"; then
        echo -e "${GREEN}OK${NC} (PID: $(ps aux | grep '[D]orisBE' | awk '{print $2}'))"
    else
        echo -e "${RED}FAIL${NC} — check log: /home/hadoop/data/logs/doris/be/be.INFO"
        exit 1
    fi
fi

echo ""
echo "=== Doris Started ==="
echo "  FE HTTP : http://$(hostname -I | awk '{print $1}'):8030"
echo "  FE Query: mysql -h 127.0.0.1 -P 9030 -u root"
echo "  BE HTTP : http://$(hostname -I | awk '{print $1}'):8040"

# ---- Auto-register BE to FE (first time) ----
sleep 3
BE_REGISTERED=$(mysql -h 127.0.0.1 -P 9030 -u root -N -e "SHOW BACKENDS\G" 2>/dev/null | grep -c "Alive: true" || echo "0")
if [ "$BE_REGISTERED" = "0" ] || [ -z "$(mysql -h 127.0.0.1 -P 9030 -u root -N -e 'SHOW BACKENDS' 2>/dev/null)" ]; then
    echo -n "[BE] Registering to FE... "
    BE_HOST=$(hostname -I | awk '{print $1}')
    mysql -h 127.0.0.1 -P 9030 -u root -e "ALTER SYSTEM ADD BACKEND '${BE_HOST}:9050';" 2>/dev/null && \
        echo -e "${GREEN}OK${NC}" || echo -e "${RED}FAIL${NC}"
fi
STARTSCRIPT
chmod +x ${DORIS_HOME}/start_doris.sh

# stop_doris.sh
cat > ${DORIS_HOME}/stop_doris.sh << 'STOPSCRIPT'
#!/bin/bash
# ============================================================================
# 停止 Apache Doris (先 BE，后 FE)
# ============================================================================
DORIS_HOME=/home/hadoop/data/doris

echo "=== Stopping Apache Doris ==="

echo -n "[BE] Stopping... "
${DORIS_HOME}/be/bin/stop_be.sh 2>/dev/null
sleep 2
if ps aux | grep -q "[D]orisBE"; then
    echo "Force killing..."
    pkill -f "doris_be" 2>/dev/null
fi
echo "OK"

echo -n "[FE] Stopping... "
${DORIS_HOME}/fe/bin/stop_fe.sh 2>/dev/null
sleep 2
if ps aux | grep -q "[D]orisFE"; then
    echo "Force killing..."
    pkill -f "DorisFE" 2>/dev/null
fi
echo "OK"

echo "=== Doris Stopped ==="
STOPSCRIPT
chmod +x ${DORIS_HOME}/stop_doris.sh

# status_doris.sh
cat > ${DORIS_HOME}/status_doris.sh << 'STATUSSCRIPT'
#!/bin/bash
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo "=== Apache Doris Status ==="
echo ""

# FE Status
echo -n "FE (Frontend): "
FE_PID=$(jps 2>/dev/null | grep DorisFE | awk '{print $1}')
if [ -n "$FE_PID" ]; then
    echo -e "${GREEN}RUNNING${NC} (PID: $FE_PID)"
    curl -s http://127.0.0.1:8030/api/bootstrap 2>/dev/null | python -m json.tool 2>/dev/null | grep -E "msg|replayedJournalId" | head -3
else
    echo -e "${RED}STOPPED${NC}"
fi

echo ""

# BE Status
echo -n "BE (Backend):  "
BE_PID=$(ps aux | grep '[D]orisBE' | awk '{print $2}' | head -1)
if [ -n "$BE_PID" ]; then
    echo -e "${GREEN}RUNNING${NC} (PID: $BE_PID)"
else
    echo -e "${RED}STOPPED${NC}"
fi

echo ""

# Cluster Info
echo "--- Cluster ---"
mysql -h 127.0.0.1 -P 9030 -u root -N -e "SHOW FRONTENDS\G" 2>/dev/null | grep -E "Name|Host|Port|Alive" || echo -e "${YELLOW}  FE not reachable${NC}"
echo ""
mysql -h 127.0.0.1 -P 9030 -u root -N -e "SHOW BACKENDS\G" 2>/dev/null | grep -E "BackendId|Host|HeartbeatPort|Alive|TotalCapacity|MemUsed" || echo -e "${YELLOW}  BE not reachable${NC}"

echo ""
echo "--- Ports ---"
echo "  FE HTTP : http://127.0.0.1:8030"
echo "  FE Query: mysql -h 127.0.0.1 -P 9030 -u root"
echo "  BE HTTP : http://127.0.0.1:8040"
STATUSSCRIPT
chmod +x ${DORIS_HOME}/status_doris.sh

echo -e "${GREEN}[OK]${NC} 管理脚本已生成"
echo ""
echo "=============================================="
echo "  部署完成!"
echo "=============================================="
echo ""
echo "  启动 Doris: sh ${DORIS_HOME}/start_doris.sh"
echo "  停止 Doris: sh ${DORIS_HOME}/stop_doris.sh"
echo "  查看状态:  sh ${DORIS_HOME}/status_doris.sh"
echo ""
echo "  FE 管理界面: http://$(hostname -I | awk '{print $1}'):8030"
echo "  MySQL 连接:   mysql -h 127.0.0.1 -P 9030 -u root"
echo ""
