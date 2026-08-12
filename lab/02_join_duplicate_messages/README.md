# 实验 02：双侧同 key 重复消息的 join —— 笛卡尔积 or 自动去重？

## 1. 实验目的

沿用统一背景（订单流 × 订单属性流，见根 `README.md`）。真实场景中两条流都可能出现同一订单号的重复消息：

- **订单流**：上游重试 / 下单时间修正，同一 `za_order_no` 出现 `apply_t` 为 `10:01`、`10:02` 两个版本
- **订单属性流**：行业服务时间回填后又被修正，同一 `za_order_no` 出现 `hyfw_start` 为 `11:00`、`11:05` 两个版本

验证问题：双侧同 key 重复消息做流式 join，是只输出 1 条结果（自动去重），还是输出 2×2=4 条全部组合（笛卡尔积）？进一步，**LEFT JOIN 和 INNER JOIN 在该场景下表现是否一致**？

## 2. 实验过程

### 2.1 环境

本机 docker compose 平台：Kafka 3.9.1（KRaft 单节点）+ Flink 2.2.1（1 JM + 1 TM），checkpoint 周期 10s（upsert-kafka 按 checkpoint 批量写出）。

### 2.2 实验设计

订单流、订单属性流各用独立 source topic；INNER / LEFT 两条 join 分支（join 条件 `o.za_order_no = p.za_order_no`）各写一个独立的 upsert-kafka sink，sink 主键 `(za_order_no, apply_t, hyfw_start)`——4 种组合的主键互不相同，若产生笛卡尔积则 4 条结果各自独立可见，不会被 upsert 合并。两个 INSERT 合并为同一个 STATEMENT SET 作业。完整 SQL 见 `demo.sql`。

一个关键实现细节：LEFT 分支用 `COALESCE(p.hyfw_start, 'EMPTY')` 兜底。因为 sink 主键列不能为 null——Flink 2.x 的 sink 自带 not-null 约束，LEFT JOIN 未匹配的补齐行若把 null 写进主键列，会抛 `EnforcerException` 让作业崩溃重启（与实验 01 的 `COALESCE(..., 'EMPTY')` 同理）。

### 2.3 测试数据与步骤

4 条消息按顺序发往两个 source topic，相邻间隔 15s（> checkpoint 周期 10s，保证每步的效果独立落盘）：

```
步骤0  → order_src  {"za_order_no":"T001","apply_t":"10:01"}    订单首个版本
步骤1  → order_src  {"za_order_no":"T001","apply_t":"10:02"}    订单修正版本
步骤2  → props_src  {"za_order_no":"T001","hyfw_start":"11:00"} 属性首次回填
步骤3  → props_src  {"za_order_no":"T001","hyfw_start":"11:05"} 属性修正版本
```

复现命令（在项目根目录执行，可反复执行，每次都从干净状态开始）：

```bash
./lab/02_join_duplicate_messages/run.sh         # 一键复现：建 topic → 取消本实验旧作业 → 清空 source/sink topic → 提交 SQL → 发消息 → 验证
./lab/02_join_duplicate_messages/run.sh T002    # 可选：自定义消息单号（默认 T001）
```

### 2.4 实际观察结果（sink topic，格式 `key <= value`）

INNER JOIN 分支（`02_join_duplicate_messages_inner_sink`），共 **4 条**：

```
(T001, 10:02, 11:00)  <=  正常消息   ← 步骤2：订单修正版 × 属性首版
(T001, 10:01, 11:00)  <=  正常消息   ← 步骤2：订单首版 × 属性首版
(T001, 10:02, 11:05)  <=  正常消息   ← 步骤3：订单修正版 × 属性修正版
(T001, 10:01, 11:05)  <=  正常消息   ← 步骤3：订单首版 × 属性修正版
```

LEFT JOIN 分支（`02_join_duplicate_messages_left_sink`），共 **8 条 = 2 条 EMPTY 补齐行 + 2 条回撤墓碑 + 4 条匹配结果**：

```
(T001, 10:01, EMPTY)  <=  正常消息   ← 步骤0：属性未回填，先补 EMPTY 行
(T001, 10:02, EMPTY)  <=  正常消息   ← 步骤1：属性未回填，先补 EMPTY 行
(T001, 10:02, EMPTY)  <=  null       ← 步骤2：回填后回撤 10:02 的补齐行（墓碑）
(T001, 10:02, 11:00)  <=  正常消息   ← 步骤2：10:02 × 11:00
(T001, 10:01, EMPTY)  <=  null       ← 步骤2：回填后回撤 10:01 的补齐行（墓碑）
(T001, 10:01, 11:00)  <=  正常消息   ← 步骤2：10:01 × 11:00
(T001, 10:02, 11:05)  <=  正常消息   ← 步骤3：10:02 × 11:05（纯新增，11:00 版本不撤）
(T001, 10:01, 11:05)  <=  正常消息   ← 步骤3：10:01 × 11:05（纯新增）
```

（实际消费到的同批次记录顺序可能与上表略有出入：同一 checkpoint 批次内按 key 缓冲，落盘顺序不保证。）

## 3. 实验结论

**是笛卡尔积，不会去重，且 LEFT JOIN 与 INNER JOIN 在此表现一致**：两条分支最终都输出了全部 4 种组合 `(10:01|10:02) × (11:00|11:05)`，一条不少。

机制解读：

1. regular join 会把两侧流的**全部历史消息**保留在各自的状态里（默认永久），每来一条新消息，就与对侧状态中所有同 key 的消息**两两匹配输出**。所以属性 `11:00` 到达时与两个订单版本各匹配一次，`11:05` 到达时再各匹配一次——不存在"同 key 已匹配过就跳过"的去重逻辑。
2. 两种 join 的差别只在**过渡态**，与实验 01 一致：订单先到、属性未回填时，LEFT JOIN 先发 `EMPTY` 补齐行，回填后回撤（墓碑）；INNER JOIN 全程静默，只在匹配时输出。一旦双侧数据到齐，两者的存量结果完全相同。
3. 推论：流式 join 双侧重复数据会被放大成 M×N 条输出（本例 2×2=4），且 join 状态永久累积。生产上若两侧都可能有重复（重试、修正版本），应在 join 前各自去重（如 `ROW_NUMBER() OVER (PARTITION BY za_order_no ORDER BY proctime)` 取最新版本），否则结果条数会随版本数乘积放大。
4. 工程注意：upsert-kafka sink 的**主键列不能为 null**，否则触发 Flink 2.x sink 的 not-null 约束（`EnforcerException`）导致作业崩溃重启；LEFT JOIN 的补齐行需对主键列 `COALESCE` 兜底。
