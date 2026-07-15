#!/bin/bash
# ============================================================================
# 游戏平台玩家行为分析与防沉迷系统 — 环境变量配置
# ============================================================================
# 使用方法: source env.sh
# ============================================================================

export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.412.b08-1.el7_9.x86_64
export HADOOP_HOME=/home/hadoop/data/hadoop
export KAFKA_HOME=/home/hadoop/data/kafka
export FLUME_HOME=/home/hadoop/data/flume
export SPARK_HOME=/home/hadoop/data/spark
export PROJECT_HOME=/home/hadoop/game_player_anti_addiction

export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$KAFKA_HOME/bin:$FLUME_HOME/bin:$SPARK_HOME/bin:$PATH

echo "=========================================="
echo "  环境已加载"
echo "=========================================="
echo "  JAVA_HOME:  $JAVA_HOME"
echo "  HADOOP_HOME: $HADOOP_HOME"
echo "  KAFKA_HOME:  $KAFKA_HOME"
echo "  FLUME_HOME:  $FLUME_HOME"
echo "  SPARK_HOME:  $SPARK_HOME"
echo "=========================================="
