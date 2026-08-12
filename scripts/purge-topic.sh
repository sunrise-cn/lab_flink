#!/usr/bin/env bash
# 清空指定 topic 的全部消息（把起始 offset 提升到高水位，不可恢复；仅适用于单分区 topic）
# 用法: ./purge-topic.sh <topic> [topic...]
set -euo pipefail
[ $# -ge 1 ] || { echo '用法: purge-topic.sh <topic> [topic...]' >&2; exit 1; }

for TOPIC in "$@"; do
  printf '{"partitions":[{"topic":"%s","partition":0,"offset":-1}],"version":1}' "$TOPIC" \
    | docker exec -i kafka sh -c 'cat > /tmp/purge-records.json'
  docker exec kafka /opt/kafka/bin/kafka-delete-records.sh \
    --bootstrap-server localhost:9092 --offset-json-file /tmp/purge-records.json >/dev/null
  echo "已清空 topic: $TOPIC"
done
