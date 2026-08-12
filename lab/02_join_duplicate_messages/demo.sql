-- 实验 02：订单流 × 订单属性流双侧重复消息的 join —— 笛卡尔积 or 自动去重？
-- 背景：订单流因上游重试/修正出现同一订单的两个版本（apply_t 10:01 / 10:02），
-- 订单属性流因属性修正出现同一订单的两个版本（hyfw_start 11:00 / 11:05），
-- 双侧 join key（za_order_no）相同。INNER / LEFT JOIN 各一条分支写入各自的
-- upsert-kafka sink，观察结果是只保留一条（自动去重），还是 2×2=4 条全部组合（笛卡尔积）

-- upsert-kafka sink 按 checkpoint 批量写出，把周期设为 10s（会话级，覆盖集群默认值）
SET 'execution.checkpointing.interval' = '10s';

-- 给作业命名，run.sh 靠它精准取消本实验的历史作业（不影响其他实验）
SET 'pipeline.name' = 'lab02-join-duplicate-messages';

CREATE TABLE order_src (
    za_order_no STRING,   -- 订单号（join key）
    apply_t     STRING    -- 申请时间：同一订单的重发/修正版本用不同时间区分
) WITH (
    'connector' = 'kafka',
    'topic' = '02_join_duplicate_messages_order_src',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'lab02-join-duplicate-messages',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

CREATE TABLE props_src (
    za_order_no STRING,   -- 订单号（join key）
    hyfw_start  STRING    -- 行业服务开始时间：同一订单的修正版本用不同时间区分
) WITH (
    'connector' = 'kafka',
    'topic' = '02_join_duplicate_messages_props_src',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'lab02-join-duplicate-messages',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

-- 主键 (za_order_no, apply_t, hyfw_start)：4 种组合互不相同，
-- 若产生笛卡尔积则 4 条结果全部独立可见，不会被 upsert 合并
CREATE TABLE inner_sink (
    za_order_no STRING,
    apply_t     STRING,
    hyfw_start  STRING,
    PRIMARY KEY (za_order_no, apply_t, hyfw_start) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = '02_join_duplicate_messages_inner_sink',
    'properties.bootstrap.servers' = 'kafka:29092',
    'key.format' = 'json',
    'value.format' = 'json'
);

CREATE TABLE left_sink (
    za_order_no STRING,
    apply_t     STRING,
    hyfw_start  STRING,
    PRIMARY KEY (za_order_no, apply_t, hyfw_start) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = '02_join_duplicate_messages_left_sink',
    'properties.bootstrap.servers' = 'kafka:29092',
    'key.format' = 'json',
    'value.format' = 'json'
);

-- 两个 INSERT 放进同一个 STATEMENT SET（同一个作业）：
-- 保证 source 只扫一次、两个分支共享数据，避免两个作业用同一 group.id 抢同一个分区的消息
EXECUTE STATEMENT SET
BEGIN
    -- INNER JOIN 分支
    INSERT INTO inner_sink
    SELECT o.za_order_no, o.apply_t, p.hyfw_start
    FROM order_src o
    INNER JOIN props_src p ON o.za_order_no = p.za_order_no;

    -- LEFT JOIN 分支（订单先到、属性未回填时先输出 hyfw_start='EMPTY' 的补齐行，回填后回撤）
    -- 注意：hyfw_start 是 sink 主键列，Flink 2.x 的 sink not-null 约束不允许主键列为 null
    -- （否则作业直接 EnforcerException 崩溃重启），未匹配时用 COALESCE(..., 'EMPTY') 兜底
    INSERT INTO left_sink
    SELECT o.za_order_no, o.apply_t, COALESCE(p.hyfw_start, 'EMPTY') AS hyfw_start
    FROM order_src o
    LEFT JOIN props_src p ON o.za_order_no = p.za_order_no;
END;
