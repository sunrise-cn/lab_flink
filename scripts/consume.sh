#!/usr/bin/env bash
# 从头消费指定 topic，格式: key <= value
# 消费 upsert-kafka sink 时，value 为 null = 回撤产生的墓碑消息(tombstone)
# 用法: ./consume.sh <topic>
set -euo pipefail

TOPIC="${1:?用法: consume.sh <topic>}"

# --timeout-ms 到期退出时 ConsoleConsumer 会打一段 ERROR/TimeoutException 噪音，过滤掉
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic "$TOPIC" \
  --from-beginning \
  --property print.key=true \
  --property key.separator='  <=  ' \
  --timeout-ms 10000 2> >(grep -vE 'ERROR Error processing message|TimeoutException' >&2)
