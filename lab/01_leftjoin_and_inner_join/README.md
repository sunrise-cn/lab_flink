# 实验 01：LEFT JOIN vs INNER JOIN 的输出差异

## 1. 实验目的

验证 Flink SQL 流式 regular join（无事件时间属性的普通 join）中：

- **LEFT JOIN**：左流先到、右流未匹配时，会**先输出一条 null 补齐行**；右流随后匹配上时，**回撤（retract）旧行、重发新行**
- **INNER JOIN**：只在两边都匹配时才输出，**无回撤、无墓碑消息**

观察手段：结果写入 upsert-kafka sink，回撤会以"该 key 的墓碑消息（value 为 null）"的形式体现在 topic 里，可直接消费验证。

## 2. 实验过程

### 2.1 环境

本机 docker compose 平台：Kafka 3.9.1（KRaft 单节点）+ Flink 2.2.1（1 JM + 1 TM），checkpoint 周期 10s（upsert-kafka 按 checkpoint 批量写出）。

### 2.2 实验设计

一个 Kafka source 同时承载两种消息（`msg_type` 区分 `order`/`props`），按 `za_order_no` 分别做 LEFT / INNER 两条 join 分支，`COALESCE(hyfw_start, apply_t)` 作为输出时间，两个 INSERT 合并为同一个 STATEMENT SET 作业写入 upsert-kafka sink（主键 `tag + za_order_no + action_time`）。完整 SQL 见 `demo.sql`。

### 2.3 测试数据与步骤

3 条消息按顺序发往 source topic，相邻间隔 15s（> checkpoint 周期 10s，保证每步的效果独立落盘）：

```
步骤0  {"msg_type":"order","za_order_no":"T001","apply_t":"10:01"}
步骤1  {"msg_type":"props","za_order_no":"T001","hyfw_start":"10:05"}
步骤2  {"msg_type":"props","za_order_no":"T001","hyfw_start":"10:02"}
```

复现命令（在项目根目录执行，可反复执行，每次都从干净状态开始）：

```bash
./lab/01_leftjoin_and_inner_join/run.sh         # 一键复现：建 topic → 取消本实验旧作业 → 清空 source/sink topic → 提交 SQL → 发消息 → 验证
./lab/01_leftjoin_and_inner_join/run.sh T002    # 可选：自定义消息单号（默认 T001）
```

### 2.4 实际观察结果（sink topic，格式 `key <= value`）

```
(left,  T001, 10:01)  <=  正常消息   ← 步骤0：LEFT 未匹配，先发 null 补齐行，COALESCE 取 apply_t
(left,  T001, 10:01)  <=  null       ← 步骤1：匹配后回撤旧行 → 墓碑消息
(left,  T001, 10:05)  <=  正常消息   ← 步骤1：重算，改取 hyfw_start
(inner, T001, 10:05)  <=  正常消息   ← 步骤1：INNER 匹配直接输出（步骤0 无输出）
(left,  T001, 10:02)  <=  正常消息   ← 步骤2：又一个版本，纯新增
(inner, T001, 10:02)  <=  正常消息   ← 步骤2：纯新增
```

共 **6 条 = left 3 正常 + 1 墓碑，inner 2 正常 + 0 墓碑**。

## 3. 实验结论

预期成立，两个 join 的本质区别如下：

| | LEFT JOIN | INNER JOIN |
|:--|:--|:--|
| 左流先到未匹配 | **立即输出**（取 `apply_t` 兜底） | 不输出 |
| 右流匹配后 | **回撤旧 key（墓碑）+ 重发新 key** | 直接输出，无回撤 |
| 再来新版本 | 纯新增，新旧并存（10:05 不撤） | 同左 |

要点解读：

1. LEFT JOIN 的"先发后撤"意味着下游会看到一个**先出现又被删除**的 key（本例 `(left,T001,10:01)`）；INNER JOIN 下游永远只看到匹配成功的结果，无中间态。
2. 回撤在 upsert-kafka 里表现为**旧主键的墓碑消息**（value=null）；新主键则是普通 upsert。主键含 `action_time`，所以 10:01 → 10:05 是"删旧增新"而非原地更新。
3. join 结果按两边流的全部历史状态做笛卡尔匹配：步骤2 的新 props 是纯新增，不影响已输出的 10:05 版本。
4. 两个工程注意事项：upsert sink 按 checkpoint 缓冲批量写，**观察消息间隔需大于 checkpoint 周期**，否则相邻效果会被合并；同一批次内多条记录的落盘顺序不保证（按 key 缓冲）。另外普通 join 的流状态默认永久保留，所以 `run.sh` 每次执行都会先取消本实验旧作业（按作业名 `lab01-leftjoin-inner-join` 匹配，不影响其他实验）并清空两个 topic（通用工具 `scripts/purge-topic.sh`），保证从干净状态重跑；若绕过 `run.sh` 手动重跑，需自行换新 join key 或重启集群。
