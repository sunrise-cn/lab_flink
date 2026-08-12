# AGENTS.md

本机 Flink SQL 学习平台：docker compose 一键启动 Kafka + Flink，按"实验（lab）"隔离验证 Flink SQL 特性。本文件约束平台的使用和扩展方式，新增/修改实验时必须遵守。

## 平台组成与运维

- Kafka 3.9.1（KRaft 单节点）：宿主机访问 `localhost:9092`，容器内访问 `kafka:29092`
- Flink 2.2.1（1 JobManager + 1 TaskManager/4 slot）：Web UI `http://localhost:8081`
- checkpoint 周期 10s（upsert-kafka sink 按 checkpoint 批量写出，这是观察 sink 消息的延迟来源）
- `./lab` 挂载到 JM 容器 `/opt/flink/lab`；`jars/` 里的 connector jar 挂载到 `/opt/flink/lib`
- 启停：`docker compose up -d` 启动；`docker compose down` 停止（**会清空 Kafka 数据和所有作业状态**）
- 提交 SQL：`docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/lab/<实验目录>/<sql文件>`，或不带 `-f` 进入交互式

## 统一实验背景（强制）

所有实验共用同一业务背景：**订单表 × 订单属性表**（保险/服务订单域，完整背景故事见根 `README.md`）：

- 订单流字段：`za_order_no`（订单号，join key）、`apply_t`（申请时间）
- 订单属性流字段：`za_order_no`（订单号，join key）、`hyfw_start`（行业服务开始时间）

新实验必须沿用该背景和字段命名造数，不要另起一套业务名词；确需新字段时在此基础上按需扩展。

## 实验规范（强制）

1. **每个实验写在 `lab/` 下的独立子目录**，目录名 = 两位序号 + 简洁含义，序号接着当前最大值递增，如 `01_leftjoin_and_inner_join`、`02_window_vs_group`。
2. **每个实验必须使用互不重复的 topic**，不允许跨实验复用。topic 名用子目录名派生，方便对应，如 `01_leftjoin_and_inner_join_src` / `01_leftjoin_and_inner_join_sink`（Kafka topic 允许字母、数字、`.`、`_`、`-`）。
3. 每个实验目录自包含 4 个文件：
   - `<实验名>.sql` — DDL + INSERT
   - `send-messages.sh` — 造测试消息，**发送前先清空 source topic**
   - `run.sh` — 一键复现（见下方"标准流程"）
   - `README.md` — 三段式实验说明书：实验目的 / 实验过程（含从项目根目录出发的复现命令）/ 实验结论（附实际观察到的记录原文）
4. SQL 内必须遵守：
   - `SET 'pipeline.name' = 'lab<序号>-<含义>';` 给作业命名，`run.sh` 靠它精准取消本实验旧作业
   - 独立的 `properties.group.id`（建议与 `pipeline.name` 一致），保证消费位点隔离
   - 多个 INSERT 必须合并进同一个 `EXECUTE STATEMENT SET ... END`（一个实验一个作业）；否则多个作业共用 group.id 消费单分区 topic 会互相抢数据
   - **VIEW 不能出现在 STATEMENT SET 里**（sql-client 的 planner bug，报 "belongs to a different planner"），需内联子查询
   - `SET 'execution.checkpointing.interval' = '10s';` 会话级兜底
5. `run.sh` 标准流程（保证每次从干净状态开始，可反复执行）：
   建 topic（幂等）→ 按 `pipeline.name` 取消本实验旧作业 → `scripts/purge-topic.sh` 清空 source/sink topic → 提交 SQL → 等 RUNNING → 发消息 → 消费 sink 验证
6. 造测试消息时，**相邻消息间隔必须大于 checkpoint 周期（10s，脚本默认 15s）**，否则相邻效果被 sink 的 checkpoint 缓冲区合并，观察不到完整过程。

## 通用工具（scripts/，与具体实验无关）

- `produce.sh <topic>` — 造数据（stdin 逐行 JSON）。**不要给它加清空逻辑**，逐条连发会互相清掉
- `consume.sh <topic>` — 从头消费，格式 `key <= value`；value 为 `null` 即回撤墓碑
- `purge-topic.sh <topic> [topic...]` — 清空 topic 全部消息（不可恢复，限单分区 topic）

## 经验记录（踩过的坑）

- upsert-kafka 的回撤 = 旧主键的墓碑消息（value=null）；主键设计决定"原地更新"还是"删旧增新"
- upsert-kafka sink 的**主键列不能为 null**：Flink 2.x sink 自带 not-null 约束，写入 null 抛 `EnforcerException` 让作业崩溃重启（可用 `'table.exec.sink.not-null-enforcer'='DROP'` 改成静默丢弃）。LEFT JOIN 的 null 补齐行必须对主键列 `COALESCE` 兜底（实验 01 的 `COALESCE(...,'EMPTY')`、实验 02 的 `COALESCE(r.seq,0)`）
- 流式 join 双侧同 key 重复消息 = 笛卡尔积放大（M×N），不去重；LEFT/INNER 一致，区别只在未匹配过渡态（实验 02）
- 普通流式 join 的左右流状态默认永久保留：重跑实验必须清理（取消旧作业 + 清 topic），否则新旧结果混杂
- 同一 checkpoint 批次内多条记录落盘顺序不保证（按 key HashMap 缓冲）
- source 默认 `scan.startup.mode = 'latest-offset'`（只消费作业启动后的新消息），需回放历史改 `earliest-offset`
- 公司平台的 `'za.datasource.type' = 'kafka'/'upsert-kafka'` 对应标准 `'connector' = 'kafka'/'upsert-kafka'`，datasource name 即 topic 名
- macOS 自带 bash 3.2 下 `$VAR` 后紧跟全角字符（如 `（`）会被并入变量名，报 `unbound variable`；脚本里统一写 `${VAR}`
- 现有实验 `lab/01_leftjoin_and_inner_join/` 是标准模板，新实验照它复制后改 `JOB_NAME`、topic、SQL 即可
