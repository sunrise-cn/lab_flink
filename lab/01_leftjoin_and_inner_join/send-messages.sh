#!/usr/bin/env bash
# 本实验的测试消息：先清空 source topic（保证从空状态开始），再依次发送 3 条，
# 相邻间隔默认 15s（需大于 checkpoint 周期 10s，
# 否则相邻两条的效果会被 upsert-kafka 的 checkpoint 缓冲区合并，观察不到完整过程）
# 用法: ./send-messages.sh [ORDER_NO] [INTERVAL]
#   ORDER_NO  默认 T001。注意：本脚本只清 topic，清不掉运行中作业的 join 状态；
#             若作业已在跑且处理过同一单号，请改用新单号（或直接 ./run.sh 一键干净重跑）
set -euo pipefail
cd "$(dirname "$0")"

TOPIC="D-oneid-chat-offline-retry-msg-202410251049"
ORDER_NO="${1:-T001}"
INTERVAL="${2:-15}"

echo '先清空 source topic 历史消息...'
../../scripts/purge-topic.sh "$TOPIC"

send() {
  echo "$1" | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 --topic "$TOPIC" >/dev/null 2>&1
  echo "[$(date +%T)] 已发送: $1"
}

send "{\"msg_type\":\"order\",\"za_order_no\":\"$ORDER_NO\",\"apply_t\":\"10:01\"}"      # 步骤0
echo "等待 ${INTERVAL}s ..."
sleep "$INTERVAL"
send "{\"msg_type\":\"props\",\"za_order_no\":\"$ORDER_NO\",\"hyfw_start\":\"10:05\"}"   # 步骤1
echo "等待 ${INTERVAL}s ..."
sleep "$INTERVAL"
send "{\"msg_type\":\"props\",\"za_order_no\":\"$ORDER_NO\",\"hyfw_start\":\"10:02\"}"   # 步骤2
echo "等待 ${INTERVAL}s 让 sink 完成最后一次 checkpoint flush ..."
sleep "$INTERVAL"
echo "发送完成。"
