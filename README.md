# 基于Hadoop的游戏平台玩家行为分析与防沉迷系统

> **课程设计项目** | 2026-06-12  
> Hadoop 3.3.6 + Flume 1.11 + Kafka 3.6.2 + Spark 3.1 + Hive 3.1 + Doris 2.0.14 + FineBI 6.0

---

## 项目简介

本项目构建了一套完整的游戏玩家行为采集、清洗、分析和可视化监控平台。系统基于国家新闻出版署《关于防止未成年人沉迷网络游戏的通知》要求，实现了未成年人游戏行为的实时监测与风险分级告警。

### 核心功能
- 📊 **实时数据采集:** Flume 采集玩家行为日志 → Kafka 流式分发 → HDFS 数据湖
- 🔍 **智能风险标注:** Spark ETL 计算 4 级防沉迷风险标签（正常/预警/违规/重度）
- ⚡ **秒级 OLAP 查询:** Apache Doris 支撑毫秒级多维聚合
- 📈 **可视化大屏:** FineBI 8 模块监控大屏，含联动钻取分析
- 🤖 **自动化运维:** 一键启停、ETL 调度、数据校验、定时告警

---

## 快速开始

### 1. 启动所有服务
```bash
sh manage.sh start        # HDFS + Kafka + Flume
sh doris/doris_manage.sh start   # Doris FE + BE
```

### 2. 查看服务状态
```bash
sh manage.sh status
sh doris/doris_manage.sh status
```

### 3. 生成测试数据并运行 ETL
```bash
python3 generate_game_logs.py 200 0.1              # 生成 200 条测试日志
sh manage.sh etl-local 20260610                    # Spark ETL (ODS→DWD)
sh doris/doris_manage.sh sync 20260610            # Hive→Doris 同步
```

### 4. 数据校验
```bash
sh verify_monitor.sh full 20260610                 # 全链路健康检查
python3 run_system_tests.py --full                  # 运行系统测试
```

### 5. 启动定时监控
```bash
sh verify_monitor.sh schedule                       # 每5分钟自动检查
```

---

## 项目结构

```
game_player_anti_addiction/
│
├── 📄 README.md                           # 本文件
├── 📄 PROJECT_COMPLETION_REPORT.md        # 项目完成报告
├── 📄 COURSE_DESIGN_REPORT.md             # 课程设计报告 (含架构/设计/测试)
│
├── 🔧 manage.sh                           # 服务管理: Hadoop/Kafka/Flume 启停
├── 🔧 env.sh                              # 环境变量
├── 🔧 start_all.sh                        # 一键启动
├── 🔧 verify_monitor.sh                   # 数据校验 + 健康监控 + 定时调度
├── 🐍 generate_game_logs.py               # 模拟玩家行为日志生成器
├── 🐍 run_system_tests.py                 # 系统测试 (正常/边界/异常/一致性)
│
├── 📁 doris/                              # Apache Doris
│   ├── doris_init.sql                     #   建库建表 DDL + 动态分区
│   ├── doris_ddl_sync_query.sql           #   DDL + Stream Load 同步 SQL
│   ├── doris_manage.sh                    #   FE/BE 启停 + 数据同步
│   ├── deploy_doris.sh                    #   部署脚本
│   └── setup_doris.sh                     #   配置脚本
│
├── 📁 finebi/                             # FineBI 可视化
│   ├── FINEBI_OPERATION_CHECKLIST.md      #   数据集+大屏配置清单
│   ├── FINEBI_DASHBOARD_STEP_BY_STEP.md   #   ★ 大屏分步制作指南 (23步)
│   ├── FINEBI_DEPLOYMENT_GUIDE.md         #   部署指南
│   └── DASHBOARD_DESIGN.md                #   大屏设计方案
│
├── 📁 spark/                              # Spark ETL
│   ├── etl_anti_addiction.py              #   PySpark 清洗+风险标注核心代码
│   └── etl_output_*.log                   #   运行日志
│
├── 📁 hive/                               # Hive 数据仓库
│   ├── hive_ddl.sql                       #   ODS/DWD/ADS 三层建表 DDL
│   └── conf/                              #   配置文件
│
├── 📁 03_flume_config/                    # Flume 配置
│   ├── game_flume.conf                    #   Agent 配置 (Taildir→Kafka+HDFS)
│   ├── start_flume.sh                     #   启动脚本
│   └── test_pipeline.sh                   #   管道测试
│
├── 📁 logs/                               # 应用日志 (Flume source)
├── 📁 reports/                            # 报告输出目录
└── 📁 test_output/                        # 测试输出目录
```

---

## 数据链路

```
generate_game_logs.py  →  Flume  →  Kafka  →  HDFS (ODS)
                                         ↓
                                    Hive ODS 表
                                         ↓
                              Spark ETL (清洗+风险标注)
                                         ↓
                               Hive DWD (ORC) + Doris DWD
                                         ↓
                              Stream Load (HTTP PUT)
                                         ↓
                                   Doris OLAP
                                         ↓
                              FineBI 可视化大屏
```

## 当前数据量
- **Doris DWD:** 8,412 行 | 1,975 位玩家
- **风险分布:** 正常 85.7% | 预警 1.2% | 违规 11.7% | 重度 1.4%
- **查询性能:** 62-112ms (KPI 聚合)

## 文档索引
| 文档 | 用途 |
|------|------|
| `PROJECT_COMPLETION_REPORT.md` | 项目状态、服务拓扑、配置修复记录 |
| `COURSE_DESIGN_REPORT.md` | 完整课程设计报告 (需求→设计→测试→总结) |
| `finebi/FINEBI_DASHBOARD_STEP_BY_STEP.md` | FineBI 大屏分步操作指南 |
| `finebi/FINEBI_OPERATION_CHECKLIST.md` | 数据集 SQL + 大屏配置速查 |
| `finebi/DASHBOARD_DESIGN.md` | 大屏架构设计 + 配色 + 布局 |

---

*注意: 服务由 hadoop 用户运行，重启机器后需手动启动所有服务。*
