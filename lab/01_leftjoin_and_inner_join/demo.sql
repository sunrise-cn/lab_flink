-- LEFT JOIN vs INNER JOIN demo
-- 说明：原 SQL 里的 'za.datasource.type'/'za.datasource.name' 是公司内部平台的封装，
-- 在本平台对应标准 connector 写法，topic 名沿用 datasource name：
--   source topic: D-oneid-chat-offline-retry-msg-202410251049
--   sink   topic: D-hanghaitu_supplier_service_role_split_inp-202608101417
-- 注意：原 SQL 的 4 个 VIEW 在这里被内联到 INSERT 里，
-- 因为 Flink 1.20 的 sql-client 在 STATEMENT SET 中引用 VIEW 会触发 planner bug
-- （AssertionError: ... belongs to a different planner）。语义完全等价。

-- upsert-kafka sink 按 checkpoint 批量写出，把周期设为 10s（会话级，覆盖集群默认值）
SET 'execution.checkpointing.interval' = '10s';

-- 给作业命名，run.sh 靠它精准取消本实验的历史作业（不影响其他实验）
SET 'pipeline.name' = 'lab01-leftjoin-inner-join';

CREATE TABLE test_source (
    msg_type    STRING,   -- 'order' 或 'props'
    za_order_no STRING,
    apply_t     STRING,   -- order 的时间
    hyfw_start  STRING    -- props 的时间
) WITH (
    'connector' = 'kafka',
    'topic' = 'D-oneid-chat-offline-retry-msg-202410251049',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-demo-join',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

CREATE TABLE test_sink (
    tag         STRING,
    za_order_no STRING,
    action_time STRING,
    PRIMARY KEY (tag, za_order_no, action_time) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'D-hanghaitu_supplier_service_role_split_inp-202608101417',
    'properties.bootstrap.servers' = 'kafka:29092',
    'key.format' = 'json',
    'value.format' = 'json'
);

-- 两个 INSERT 放进同一个 STATEMENT SET（同一个作业）：
-- 保证 source 只扫一次、两个分支共享数据，避免两个作业用同一 group.id 抢同一个分区的消息
-- COALESCE(p.hyfw_start, o.apply_t, 'EMPTY') 等价于原 SQL 的
-- 内层 COALESCE(p.hyfw_start, o.apply_t) + 外层 COALESCE(action_time, 'EMPTY')
EXECUTE STATEMENT SET
BEGIN
    -- LEFT JOIN 分支（原 v_left）
    INSERT INTO test_sink
    SELECT 'left' AS tag, o.za_order_no,
        COALESCE(p.hyfw_start, o.apply_t, 'EMPTY') AS action_time
    FROM (SELECT za_order_no, apply_t FROM test_source WHERE msg_type = 'order') o
    LEFT JOIN (SELECT za_order_no, hyfw_start FROM test_source WHERE msg_type = 'props') p
        ON o.za_order_no = p.za_order_no;

    -- INNER JOIN 分支（原 v_inner）
    INSERT INTO test_sink
    SELECT 'inner' AS tag, o.za_order_no,
        COALESCE(p.hyfw_start, o.apply_t, 'EMPTY') AS action_time
    FROM (SELECT za_order_no, apply_t FROM test_source WHERE msg_type = 'order') o
    INNER JOIN (SELECT za_order_no, hyfw_start FROM test_source WHERE msg_type = 'props') p
        ON o.za_order_no = p.za_order_no;
END;
