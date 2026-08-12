#!/usr/bin/env bash
# 通用造数工具：向指定 topic 发送消息
# 用法1: ./scripts/produce.sh <topic>          然后逐行输入 JSON，Ctrl+D 结束
# 用法2: echo '{"k":"v"}' | ./scripts/produce.sh <topic>
set -euo pipefail

TOPIC="${1:?用法: produce.sh <topic>}"

docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic "$TOPIC"
