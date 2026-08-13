-- 实验 04：水位线（watermark）在 window 和 join 中的作用
-- 一个作业两条分支，观察"结果什么时候才落盘"：
--   分支1：订单流 1 分钟滚动窗口计数（TUMBLE window，单输入算子）
--   分支2：订单 LEFT JOIN 订单属性 ±1min（interval join，双输入算子）
-- 关键对照（配合 run.sh 分阶段发消息 + 分阶段消费 sink 观察）：
--   a) 窗口数据到齐但水位线没过窗口结束时间 → 窗口结果不输出
--   b) join 匹配成功 → 立即输出，不等水位线
--   c) join 未匹配的 null 补齐行 → 等水位线越过窗口右边界才输出
--   d) 双输入算子水位线取两侧最小值 → 只推一条流的水位线没用

-- upsert-kafka sink 按 checkpoint 批量写出，把周期设为 10s（会话级，覆盖集群默认值）
SET 'execution.checkpointing.interval' = '10s';

-- 给作业命名，run.sh 靠它精准取消本实验的历史作业（不影响其他实验）
SET 'pipeline.name' = 'lab04-watermark-window-join';

-- 订单流：事件时间 = apply_t，水位线允许 5s 乱序（watermark = 事件时间 - 5s）
CREATE TABLE order_src (
    za_order_no STRING,   -- 订单号（join key）
    apply_t     STRING,   -- 申请时间（原始字符串，用于展示）
    apply_ts    AS TO_TIMESTAMP(apply_t, 'yyyy-MM-dd HH:mm:ss'),
    WATERMARK FOR apply_ts AS apply_ts - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = '04_watermark_window_join_order_src',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'lab04-watermark-window-join',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

-- 订单属性流：事件时间 = hyfw_start，水位线同样允许 5s 乱序
CREATE TABLE props_src (
    za_order_no STRING,   -- 订单号（join key）
    hyfw_start  STRING,   -- 行业服务开始时间（原始字符串，用于展示）
    hyfw_ts     AS TO_TIMESTAMP(hyfw_start, 'yyyy-MM-dd HH:mm:ss'),
    WATERMARK FOR hyfw_ts AS hyfw_ts - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = '04_watermark_window_join_props_src',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'lab04-watermark-window-join',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

CREATE TABLE window_sink (
    window_start TIMESTAMP(3),
    window_end   TIMESTAMP(3),
    order_cnt    BIGINT,
    PRIMARY KEY (window_start) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = '04_watermark_window_join_window_sink',
    'properties.bootstrap.servers' = 'kafka:29092',
    'key.format' = 'json',
    'value.format' = 'json'
);

CREATE TABLE join_sink (
    za_order_no STRING,
    apply_t     STRING,
    hyfw_start  STRING,
    PRIMARY KEY (za_order_no, apply_t, hyfw_start) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = '04_watermark_window_join_join_sink',
    'properties.bootstrap.servers' = 'kafka:29092',
    'key.format' = 'json',
    'value.format' = 'json'
);

-- 两个 INSERT 放进同一个 STATEMENT SET（同一个作业）：
-- 保证 source 只扫一次、两个分支共享数据，避免两个作业用同一 group.id 抢同一个分区的消息
EXECUTE STATEMENT SET
BEGIN
    -- 分支1：订单流的 1 分钟事件时间滚动窗口计数
    INSERT INTO window_sink
    SELECT window_start, window_end, COUNT(*) AS order_cnt
    FROM TABLE(TUMBLE(TABLE order_src, DESCRIPTOR(apply_ts), INTERVAL '1' MINUTE))
    GROUP BY window_start, window_end;

    -- 分支2：订单 LEFT JOIN 订单属性（前后 1 分钟事件时间窗口）
    -- 主键列不能为 null（Flink 2.x sink not-null 约束），未匹配用 'EMPTY' 兜底
    INSERT INTO join_sink
    SELECT o.za_order_no, o.apply_t, COALESCE(p.hyfw_start, 'EMPTY') AS hyfw_start
    FROM order_src o
    LEFT JOIN props_src p
        ON o.za_order_no = p.za_order_no
        AND o.apply_ts BETWEEN p.hyfw_ts - INTERVAL '1' MINUTE AND p.hyfw_ts + INTERVAL '1' MINUTE;
END;
