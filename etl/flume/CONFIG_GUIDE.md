# Flume 配置与部署说明

## 一、文件清单

```
flume/
├── flume.conf          # Flume Agent 完整配置文件
├── start_flume.sh      # 启动脚本（可直接运行）
├── position/           # Taildir Source 断点续传位置记录目录（自动创建）
└── CONFIG_GUIDE.md     # 本说明文档
```

## 二、集群节点与端口（通用配置，可直接修改）

| 组件 | 配置项 | 默认值 | 说明 |
|------|--------|--------|------|
| Kafka | bootstrap.servers | `kafka-node1:9092,kafka-node2:9092,kafka-node3:9092` | Kafka集群Broker列表 |
| Kafka | topic | `game_player_behavior` | 目标Topic名称 |
| HDFS | NameNode | `hdfs://namenode:9000` | Hadoop NameNode地址 |
| HDFS | 根路径 | `/user/flume/game_logs/dt=%Y%m%d` | 按日期分目录存储 |
| Flume | Agent名称 | `a1` | Agent标识，与配置文件中保持一致 |

## 三、数据流架构

```
Python日志生成器                     Flume Agent (a1)
     |                                    |
     v                                    v
game_player_behavior.log ──→ Taildir Source (r1)
     (实时追加)                     │
                                    ├── Interceptor: timestamp + host
                                    │
                                    └── Channel Selector: replicating
                                           │
                           ┌───────────────┴───────────────┐
                           v                               v
                    Memory Channel (c1)            Memory Channel (c2)
                           │                               │
                           v                               v
                      Kafka Sink (k1)                HDFS Sink (k2)
                           │                               │
                           v                               v
                    Kafka Topic:                  HDFS 按日存储:
                    game_player_behavior          /user/flume/game_logs/
                                                   dt=20260610/
                                                   game_player_behavior.xxx.log
```

## 四、启动命令

### 4.1 直接启动（前台，Ctrl+C 停止）

```bash
cd /home/hadoop/game_player_anti_addiction/flume
sh start_flume.sh
```

### 4.2 后台启动（推荐生产环境）

```bash
cd /home/hadoop/game_player_anti_addiction/flume
nohup sh start_flume.sh > flume_output.log 2>&1 &
```

### 4.3 直接使用 flume-ng 命令

```bash
/usr/local/flume/bin/flume-ng agent \
    --name a1 \
    --conf /usr/local/flume/conf \
    --conf-file /home/hadoop/game_player_anti_addiction/flume/flume.conf \
    -Dflume.root.logger=INFO,console \
    -Dflume.monitoring.type=http \
    -Dflume.monitoring.port=34545 \
    -Duser.timezone=Asia/Shanghai
```

### 4.4 参数说明

| 参数 | 说明 |
|------|------|
| `--name a1` | Agent名称，必须与配置文件中定义的名称一致 |
| `--conf` | Flume自身配置目录（flume-env.sh所在目录） |
| `--conf-file` | 业务配置文件路径，即flume.conf |
| `-Dflume.root.logger` | 日志级别与输出位置（INFO,console / DEBUG,console） |
| `-Dflume.monitoring.type` | 监控类型，http可提供JSON监控接口 |
| `-Dflume.monitoring.port` | 监控HTTP端口，访问 `http://host:34545/metrics` 查看 |
| `-Duser.timezone` | 时区，影响HDFS目录中的日期分区 |

### 4.5 验证启动

```bash
# 查看Flume进程
jps -l | grep flume

# 查看监控指标
curl http://localhost:34545/metrics

# 查看日志输出
tail -f flume_output.log
```

## 五、关键配置说明

### 5.1 Taildir Source — 断点续传

- `positionFile` 记录每个被监控文件的 `inode + offset`
- Agent 重启后从上次位置继续读取，**不会丢失或重复数据**
- 如需重新采集历史数据，删除 position 文件即可：
  ```bash
  rm /home/hadoop/game_player_anti_addiction/flume/position/taildir_position.json
  ```

### 5.2 Replicating 通道选择器

- 一份数据**同时**写入 c1 和 c2 两个通道
- c1 → Kafka（实时流处理：Spark Streaming / Flink）
- c2 → HDFS（离线批处理：Hive / Spark SQL）
- 两个通道独立运行，互不阻塞

### 5.3 HDFS 文件滚动策略

| 策略项 | 值 | 说明 |
|--------|-----|------|
| rollSize | 128MB | 文件达到128MB滚动，与HDFS块对齐 |
| rollCount | 100,000 | 10万条Event后滚动 |
| rollInterval | 600s | 10分钟超时强制滚动 |
| idleTimeout | 360s | 6分钟无数据则关闭文件 |

**触发机制**：任一条件先满足即滚动。例如：数据量达到128MB但时间只过了5分钟→立即滚动；数据量只有1MB但已过10分钟→强制滚动。

### 5.4 Kafka 生产者配置

| 参数 | 值 | 说明 |
|------|-----|------|
| acks | 1 | Leader确认即返回，平衡可靠性与延迟 |
| compression | snappy | Snappy压缩，减少网络传输量 |
| retries | 3 | 发送失败重试3次 |
| batch.size | 16KB | 批量发送大小 |

## 六、环境依赖检查

```bash
# 1. 确认 Flume 安装
ls /usr/local/flume/bin/flume-ng

# 2. 确认 Kafka 客户端 JAR（Flume 1.9 自带，检查是否存在）
ls /usr/local/flume/lib/kafka-clients-*.jar

# 3. 确认 Hadoop 客户端 JAR
ls /usr/local/flume/lib/hadoop-common-*.jar
ls /usr/local/flume/lib/hadoop-hdfs-*.jar

# 4. 确认日志生成脚本已运行
python3 /home/hadoop/game_player_anti_addiction/generate_game_logs.py 10000 0.5

# 5. 确认 Kafka Topic 已创建
kafka-topics.sh --create --topic game_player_behavior --bootstrap-server kafka-node1:9092 --partitions 3 --replication-factor 2

# 6. 确认 HDFS 目录权限
hdfs dfs -mkdir -p /user/flume/game_logs
hdfs dfs -chmod 755 /user/flume/game_logs
```

## 七、常见问题

| 问题 | 可能原因 | 解决方法 |
|------|----------|----------|
| ChannelFullException | 内存通道容量不足 | 增大 `capacity` 或减小 Sink 批量大小 |
| HDFS连接超时 | NameNode不可达 | 检查 `hdfs://namenode:9000` 地址和网络 |
| Kafka Sink无输出 | Topic不存在或Broker不可达 | 确认Topic已创建，Broker端口开放 |
| position文件损坏 | 异常关机 | 删除 position 文件，从头开始采集 |
| 小文件过多 | rollSize/rollInterval 设置过小 | 增大 rollSize 至128MB+，rollInterval至600s+ |
