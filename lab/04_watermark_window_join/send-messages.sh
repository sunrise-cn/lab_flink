#!/usr/bin/env bash
# 本实验的测试消息：8 条消息分 6 个阶段发送，配合 run.sh 逐阶段消费 sink 观察。
# 阶段内相邻消息只隔 3s（同一观察批次）；阶段之间由 run.sh 消费并等 15s（> checkpoint 周期 10s）。
# 用法: ./send-messages.sh [STAGE]
#   STAGE=all（默认）：清空 source topic 后一次发完全部 8 条（不想分阶段观察时用）
#   STAGE=1..6     ：只发某一阶段的消息（run.sh 逐阶段调用）；STAGE=1 会先清空 source topic
set -euo pipefail
cd "$(dirname "$0")"

ORDER_TOPIC='04_watermark_window_join_order_src'
PROPS_TOPIC='04_watermark_window_join_props_src'
STAGE="${1:-all}"

send() {
  echo "$2" | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 --topic "$1" >/dev/null 2>&1
  echo "[$(date +%T)] 已发送($(basename "$1")): $2"
  sleep 3   # 保证跨 topic 消息的处理顺序与叙述一致
}

case "$STAGE" in
  all)
    ../../scripts/purge-topic.sh "$ORDER_TOPIC" "$PROPS_TOPIC"
    for S in 1 2 3 4 5 6; do
      echo "-- 阶段 $S"
      "$0" "$S"
      echo '等待 15s 让 sink 完成 checkpoint flush ...'
      sleep 15
    done
    echo '发送完成。'
    ;;
  1)
    # 窗口组：同一窗口 [10:00,10:01) 的两条订单。发完后水位线=10:00:35，没过窗口结束 10:01:00
    ../../scripts/purge-topic.sh "$ORDER_TOPIC" "$PROPS_TOPIC"
    send "$ORDER_TOPIC" '{"za_order_no":"T001","apply_t":"2026-08-13 10:00:10"}'
    send "$ORDER_TOPIC" '{"za_order_no":"T002","apply_t":"2026-08-13 10:00:40"}'
    ;;
  2)
    # 推水位线：10:02:00 - 5s = 10:01:55 > 10:01:00，窗口 [10:00,10:01) 关门输出
    send "$ORDER_TOPIC" '{"za_order_no":"T003","apply_t":"2026-08-13 10:02:00"}'
    ;;
  3)
    # join 匹配组：属性先到、订单晚 30s（±1min 窗口内），匹配成功立即输出
    send "$PROPS_TOPIC" '{"za_order_no":"T101","hyfw_start":"2026-08-13 10:10:20"}'
    send "$ORDER_TOPIC" '{"za_order_no":"T101","apply_t":"2026-08-13 10:10:50"}'
    ;;
  4)
    # join 未匹配组 + 只推订单流水位线：
    # T102 无属性，null 行要等算子水位线 > 10:21:00；
    # T103 把订单流水位线推到 10:21:55，但属性流还停在 10:10:15，双输入取最小值 → 仍不放行
    send "$ORDER_TOPIC" '{"za_order_no":"T102","apply_t":"2026-08-13 10:20:00"}'
    send "$ORDER_TOPIC" '{"za_order_no":"T103","apply_t":"2026-08-13 10:22:00"}'
    ;;
  5)
    # 推属性流水位线：10:24:00 - 5s = 10:23:55；算子水位线 = min(10:21:55, 10:23:55) = 10:21:55
    # > 10:21:00 → T102 的 null 补齐行放行
    send "$PROPS_TOPIC" '{"za_order_no":"T999","hyfw_start":"2026-08-13 10:24:00"}'
    ;;
  6)
    # 收尾：与 T999 属性互配（立即输出）；订单流水位线推到 10:24:25，
    # 算子水位线 = min(10:24:25, 10:23:55) = 10:23:55 > 10:23:00 → T103 的 null 行放行
    send "$ORDER_TOPIC" '{"za_order_no":"T999","apply_t":"2026-08-13 10:24:30"}'
    ;;
  *)
    echo "用法: $0 [all|1|2|3|4|5|6]" >&2
    exit 1
    ;;
esac
