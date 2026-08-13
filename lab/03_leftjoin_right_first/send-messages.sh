#!/usr/bin/env bash
# 本实验的测试消息：先清空两个 source topic（保证从空状态开始），再依次发送 6 条，
# 相邻间隔默认 15s（需大于 checkpoint 周期 10s，
# 否则相邻两条的效果会被 upsert-kafka 的 checkpoint 缓冲区合并，观察不到完整过程）
# 用法: ./send-messages.sh [INTERVAL]
set -euo pipefail
cd "$(dirname "$0")"

ORDER_TOPIC='03_leftjoin_right_first_order_src'
PROPS_TOPIC='03_leftjoin_right_first_props_src'
INTERVAL="${1:-15}"

echo '先清空 source topic 历史消息...'
../../scripts/purge-topic.sh "$ORDER_TOPIC" "$PROPS_TOPIC"

send() {
  echo "$2" | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 --topic "$1" >/dev/null 2>&1
  echo "[$(date +%T)] 已发送($1): $2"
}

# 组1（窗口内）：属性先到，订单晚 30s
send "$PROPS_TOPIC" '{"za_order_no":"T001","hyfw_start":"2026-08-13 10:00:00"}'  # 步骤0：属性先到
echo "等待 ${INTERVAL}s ..."
sleep "$INTERVAL"
send "$ORDER_TOPIC" '{"za_order_no":"T001","apply_t":"2026-08-13 10:00:30"}'     # 步骤1：订单晚 30s（±1min 窗口内）
echo "等待 ${INTERVAL}s ..."
sleep "$INTERVAL"
# 组2（超窗口）：属性先到，订单晚 2min
send "$PROPS_TOPIC" '{"za_order_no":"T002","hyfw_start":"2026-08-13 11:00:00"}'  # 步骤2：属性先到
echo "等待 ${INTERVAL}s ..."
sleep "$INTERVAL"
send "$ORDER_TOPIC" '{"za_order_no":"T002","apply_t":"2026-08-13 11:02:00"}'     # 步骤3：订单晚 2min（超 ±1min 窗口）
echo "等待 ${INTERVAL}s ..."
sleep "$INTERVAL"
# 收尾：两条流各发一条晚事件推进水位线（interval join 双输入水位线取两侧最小值，两条流都要推），
# 使 T002 的 interval 未匹配 null 行得以输出；T999 自身互配，兼作对照
send "$PROPS_TOPIC" '{"za_order_no":"T999","hyfw_start":"2026-08-13 11:04:00"}'  # 步骤4：推水位线
echo "等待 ${INTERVAL}s ..."
sleep "$INTERVAL"
send "$ORDER_TOPIC" '{"za_order_no":"T999","apply_t":"2026-08-13 11:04:30"}'     # 步骤5：推水位线
echo "等待 ${INTERVAL}s 让 sink 完成最后一次 checkpoint flush ..."
sleep "$INTERVAL"
echo "发送完成。"
