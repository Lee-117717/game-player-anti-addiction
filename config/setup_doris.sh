#!/bin/bash
# ============================================================================
# Apache Doris 2.0.4 单节点部署脚本 (FE + BE)
# 适配虚拟机低资源环境
# ============================================================================
set -e

DORIS_SRC=${1:-/tmp/apache-doris-2.0.0-bin-x64.tar.gz}
DORIS_BASE=/home/hadoop/data/doris
JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.412.b08-1.el7_9.x86_64}

echo "=========================================="
echo "  Apache Doris 单节点部署"
echo "=========================================="

# ---- 1. 提取 ----
if [ ! -d "${DORIS_BASE}/fe" ]; then
    echo "[1/5] Extracting Doris..."
    tar -xzf $DORIS_SRC -C /home/hadoop/data/
    # Rename to standard layout
    mv /home/hadoop/data/apache-doris-2.0.4-bin-x64 ${DORIS_BASE}
fi
echo "  Doris extracted to: ${DORIS_BASE}"

# ---- 2. FE 配置 ----
echo "[2/5] Configuring FE..."
FE_CONF=${DORIS_BASE}/fe/conf/fe.conf
# 备份原始配置
cp ${FE_CONF} ${FE_CONF}.bak 2>/dev/null || true

cat > ${FE_CONF} << 'FECONF'
# Doris FE 配置 — 单节点低资源模式
LOG_DIR = /home/hadoop/data/doris/fe/log
DATE = "$(date +%Y%m%d-%H%M%S)"
JAVA_HOME = /usr/lib/jvm/java-1.8.0-openjdk-1.8.0.412.b08-1.el7_9.x86_64
JAVA_OPTS = -Xms256m -Xmx512m -XX:+UseSerialGC -XX:MaxGCPauseMillis=500 -Dfile.encoding=UTF-8
JAVA_OPTS_FOR_JDK_9 = -Xms256m -Xmx512m -XX:+UseSerialGC -XX:MaxGCPauseMillis=500 -Dfile.encoding=UTF-8

# 单节点: 不启用高可用
meta_dir = /home/hadoop/data/doris/fe/doris-meta
# 单副本模式 (生产环境需改为3)
metadata_failure_recovery = false
# 降低心跳超时
heartbeat_timeout_second = 120
# 端口配置
http_port = 8030
rpc_port = 9020
query_port = 9030
edit_log_port = 9010

# 低资源调优
qe_max_connection = 100
max_running_txn_num_per_db = 100
FECONF

# ---- 3. BE 配置 ----
echo "[3/5] Configuring BE..."
BE_CONF=${DORIS_BASE}/be/conf/be.conf
cp ${BE_CONF} ${BE_CONF}.bak 2>/dev/null || true

# 获取本机IP
LOCAL_IP=$(hostname -I 2>/dev/null | awk '{print $1}' || echo "127.0.0.1")

cat > ${BE_CONF} << BECONF
# Doris BE 配置 — 单节点低资源模式
be_port = 9060
webserver_port = 8040
heartbeat_service_port = 9050
brpc_port = 8060

# 存储路径 (确保目录存在且有空间)
storage_root_path = /home/hadoop/data/doris/be/storage
# 单副本模式
replication_num = 1

# 低内存配置 (关键!)
mem_limit = 50%
max_base_compaction_concurrency = 1
max_cumu_compaction_concurrency = 1

# BE JVM
JAVA_HOME = /usr/lib/jvm/java-1.8.0-openjdk-1.8.0.412.b08-1.el7_9.x86_64
JAVA_OPTS = -Xms256m -Xmx512m -XX:+UseSerialGC -Dfile.encoding=UTF-8
BECONF

# 创建存储目录
mkdir -p ${DORIS_BASE}/be/storage

# ---- 4. 设置权限 ----
echo "[4/5] Setting permissions..."
chmod +x ${DORIS_BASE}/fe/bin/*.sh 2>/dev/null || true
chmod +x ${DORIS_BASE}/be/bin/*.sh 2>/dev/null || true

# ---- 5. 创建启动脚本 ----
echo "[5/5] Creating start/stop scripts..."

# 启动脚本
cat > ${DORIS_BASE}/start_doris.sh << 'START'
#!/bin/bash
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.412.b08-1.el7_9.x86_64
DORIS_BASE=/home/hadoop/data/doris
echo "Starting Doris FE..."
${DORIS_BASE}/fe/bin/start_fe.sh --daemon
sleep 5
echo "Starting Doris BE..."
${DORIS_BASE}/be/bin/start_be.sh --daemon
sleep 5
# 检查状态
if ps aux | grep -q "[d]oris.fe"; then echo "  FE: OK"; else echo "  FE: FAIL"; fi
if ps aux | grep -q "[d]oris.be"; then echo "  BE: OK"; else echo "  BE: FAIL"; fi
echo "Doris started."
START

# 停止脚本
cat > ${DORIS_BASE}/stop_doris.sh << 'STOP'
#!/bin/bash
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.412.b08-1.el7_9.x86_64
DORIS_BASE=/home/hadoop/data/doris
echo "Stopping Doris BE..."
${DORIS_BASE}/be/bin/stop_be.sh 2>/dev/null || true
sleep 2
echo "Stopping Doris FE..."
${DORIS_BASE}/fe/bin/stop_fe.sh 2>/dev/null || true
sleep 2
pkill -f "doris.fe" 2>/dev/null || true
pkill -f "doris.be" 2>/dev/null || true
echo "Doris stopped."
STOP

chmod +x ${DORIS_BASE}/start_doris.sh ${DORIS_BASE}/stop_doris.sh

echo ""
echo "=========================================="
echo "  Doris 部署完成!"
echo "=========================================="
echo "  启动: sh ${DORIS_BASE}/start_doris.sh"
echo "  停止: sh ${DORIS_BASE}/stop_doris.sh"
echo "  检查: mysql -h 127.0.0.1 -P 9030 -u root"
echo "=========================================="
