-- 实验 03：LEFT JOIN 右流（订单属性）先到、左流（订单）后到，还能匹配上吗？
-- 情况1：普通 LEFT JOIN（无时间窗口，双侧状态默认永久保留）
-- 情况2：LEFT JOIN + 前后 1 分钟事件时间窗口（interval join，双侧状态按窗口保留）
-- 对照组：左流晚到 30s（窗口内） vs 左流晚到 2min（超出窗口），验证两种情况是否一致

-- upsert-kafka sink 按 checkpoint 批量写出，把周期设为 10s（会话级，覆盖集群默认值）
SET 'execution.checkpointing.interval' = '10s';

-- 给作业命名，run.sh 靠它精准取消本实验的历史作业（不影响其他实验）
SET 'pipeline.name' = 'lab03-leftjoin-right-first';

-- 订单流：事件时间 = apply_t（申请时间），水位线允许 5s 乱序
CREATE TABLE order_src (
    za_order_no STRING,   -- 订单号（join key）
    apply_t     STRING,   -- 申请时间（原始字符串，用于展示）
    apply_ts    AS TO_TIMESTAMP(apply_t, 'yyyy-MM-dd HH:mm:ss'),
    WATERMARK FOR apply_ts AS apply_ts - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = '03_leftjoin_right_first_order_src',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'lab03-leftjoin-right-first',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

-- 订单属性流：事件时间 = hyfw_start（行业服务开始时间），水位线允许 5s 乱序
CREATE TABLE props_src (
    za_order_no STRING,   -- 订单号（join key）
    hyfw_start  STRING,   -- 行业服务开始时间（原始字符串，用于展示）
    hyfw_ts     AS TO_TIMESTAMP(hyfw_start, 'yyyy-MM-dd HH:mm:ss'),
    WATERMARK FOR hyfw_ts AS hyfw_ts - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = '03_leftjoin_right_first_props_src',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'lab03-leftjoin-right-first',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

CREATE TABLE regular_sink (
    za_order_no STRING,
    apply_t     STRING,
    hyfw_start  STRING,
    PRIMARY KEY (za_order_no, apply_t, hyfw_start) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = '03_leftjoin_right_first_regular_sink',
    'properties.bootstrap.servers' = 'kafka:29092',
    'key.format' = 'json',
    'value.format' = 'json'
);

CREATE TABLE interval_sink (
    za_order_no STRING,
    apply_t     STRING,
    hyfw_start  STRING,
    PRIMARY KEY (za_order_no, apply_t, hyfw_start) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = '03_leftjoin_right_first_interval_sink',
    'properties.bootstrap.servers' = 'kafka:29092',
    'key.format' = 'json',
    'value.format' = 'json'
);

-- 两个 INSERT 放进同一个 STATEMENT SET（同一个作业）：
-- 保证 source 只扫一次、两个分支共享数据，避免两个作业用同一 group.id 抢同一个分区的消息
EXECUTE STATEMENT SET
BEGIN
    -- 情况1：普通 LEFT JOIN，无时间窗口
    -- 主键列不能为 null（Flink 2.x sink not-null 约束），未匹配用 'EMPTY' 兜底
    INSERT INTO regular_sink
    SELECT o.za_order_no, o.apply_t, COALESCE(p.hyfw_start, 'EMPTY') AS hyfw_start
    FROM order_src o
    LEFT JOIN props_src p ON o.za_order_no = p.za_order_no;

    -- 情况2：LEFT JOIN + 前后 1 分钟事件时间窗口（interval join）
    INSERT INTO interval_sink
    SELECT o.za_order_no, o.apply_t, COALESCE(p.hyfw_start, 'EMPTY') AS hyfw_start
    FROM order_src o
    LEFT JOIN props_src p
        ON o.za_order_no = p.za_order_no
        AND o.apply_ts BETWEEN p.hyfw_ts - INTERVAL '1' MINUTE AND p.hyfw_ts + INTERVAL '1' MINUTE;
END;
