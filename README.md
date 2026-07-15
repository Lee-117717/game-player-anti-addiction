# 基于Hadoop的游戏平台玩家行为分析与防沉迷系统

> **课程设计项目** | 2026-06-12  
> Hadoop 3.3.6 + Flume 1.11 + Kafka 3.6.2 + Spark 3.1 + Hive 3.1 + Doris 2.0.14

---

## 项目简介

本项目构建了一套完整的游戏玩家行为采集、清洗、分析和可视化监控平台。系统基于国家新闻出版署《关于防止未成年人沉迷网络游戏的通知》要求，实现了未成年人游戏行为的实时监测与风险分级告警。

### 核心功能
- 📊 **实时数据采集:** Flume 采集玩家行为日志 → Kafka 流式分发 → HDFS 数据湖
- 🔍 **智能风险标注:** Spark ETL 计算 4 级防沉迷风险标签（正常/预警/违规/重度）
- ⚡ **秒级 OLAP 查询:** Apache Doris 支撑毫秒级多维聚合
- 📈 **可视化大屏:** Vue.js + Spring Boot 自建 8 模块监控大屏，含联动钻取分析
- 🤖 **自动化运维:** 一键启停、ETL 调度、数据校验、定时告警

---

## 快速开始

### 1. 启动所有服务
```bash
sh scripts/manage.sh start                    # HDFS + Kafka + Flume
sh config/doris_manage.sh start               # Doris FE + BE
```

### 2. 查看服务状态
```bash
sh scripts/manage.sh status
sh config/doris_manage.sh status
```

### 3. 生成测试数据并运行 ETL
```bash
python3 tools/generate_game_logs.py 200 0.1               # 生成 200 条测试日志
sh scripts/manage.sh etl-local 20260610                   # Spark ETL (ODS→DWD)
sh config/doris_manage.sh sync 20260610                   # Hive→Doris 同步
```

### 4. 数据校验
```bash
sh scripts/verify_monitor.sh full 20260610                # 全链路健康检查
python3 tools/run_system_tests.py --full                   # 运行系统测试
```

### 5. 启动定时监控
```bash
sh scripts/verify_monitor.sh schedule                      # 每5分钟自动检查
```

---

## 项目结构

```
game-player-anti-addiction/
│
├── README.md
├── pom.xml                                # Maven 构建配置
│
├── src/                                   # Spring Boot 后端
│   └── main/
│       ├── java/com/game/antidote/
│       │   ├── AntiAddictionApplication.java
│       │   ├── common/                    # 统一返回结果
│       │   ├── config/                    # Web/Scheduled 配置
│       │   ├── controller/                # Dashboard API 控制器
│       │   ├── entity/                    # 8 个数据实体
│       │   ├── mapper/                    # MyBatis 映射接口
│       │   └── service/                   # 业务逻辑层
│       └── resources/
│           ├── application.yml            # 应用配置
│           ├── mapper/                    # MyBatis XML
│           └── static/                    # 大屏前端构建产物
│
├── dashboard/                             # Vue.js 大屏前端源码
│   ├── src/
│   │   ├── views/Dashboard.vue            # 监控大屏主页面
│   │   ├── components/                    # KpiCard/BaseEchart/BaseTable 等
│   │   └── api/                           # 后端 API 调用
│   ├── package.json
│   └── vite.config.js
│
├── etl/                                   # ETL 数据处理
│   ├── spark/etl_anti_addiction.py        # PySpark 清洗+风险标注
│   ├── flume/                             # Flume 采集配置
│   │   ├── flume.conf                     # 标准 Agent 配置
│   │   ├── game_flume.conf                # 游戏日志采集配置
│   │   ├── start_flume.sh                 # 启动脚本
│   │   └── test_pipeline.sh               # 管道测试
│   └── lightweight_pipeline.py            # 轻量数据处理管线
│
├── sql/                                   # 数据库定义与查询
│   ├── hive_ddl.sql                       # Hive ODS/DWD/ADS 三层 DDL
│   ├── doris_init.sql                     # Doris 建库建表 + 动态分区
│   ├── doris_ddl_sync_query.sql           # DDL + Stream Load 同步
│   └── dashboard_queries.sql              # 大屏 8 组 API 查询 SQL
│
├── config/                                # 组件部署与配置
│   ├── hive-site.xml                      # Hive 配置
│   ├── deploy_doris.sh                    # Doris 部署脚本
│   ├── doris_manage.sh                    # Doris FE/BE 管理
│   └── setup_doris.sh                     # Doris 初始化
│
├── scripts/                               # 运维管理脚本
│   ├── manage.sh                          # 服务管理 (HDFS/Kafka/Flume)
│   ├── start_all.sh                       # 一键启动所有服务
│   ├── auto_startup.sh                    # 开机自启
│   ├── auto_pipeline.sh                   # 自动 ETL 调度
│   ├── verify_monitor.sh                  # 数据校验 + 健康监控
│   ├── init_doris_data.sh                 # Doris 数据初始化
│   └── env.sh                             # 环境变量
│
├── tools/                                 # Python 工具集
│   ├── generate_game_logs.py              # 模拟玩家行为日志生成器
│   ├── generate_doris_test_data.py        # Doris 测试数据生成
│   └── run_system_tests.py                # 系统测试 (正常/边界/异常/一致性)
│
└── docs/                                  # 文档与报告
    ├── 实验报告.docx                       # 课程设计报告
    └── reports/                           # 测试与健康检查报告
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
                    Spring Boot API → Vue.js 可视化大屏
```

## 当前数据量
- **Doris DWD:** 8,412 行 | 1,975 位玩家
- **风险分布:** 正常 85.7% | 预警 1.2% | 违规 11.7% | 重度 1.4%
- **查询性能:** 62-112ms (KPI 聚合)

---

*注意: 服务由 hadoop 用户运行，重启机器后需手动启动所有服务。*
