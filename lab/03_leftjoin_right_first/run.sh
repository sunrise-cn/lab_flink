#!/usr/bin/env bash
# 一键跑通本实验（每次从干净状态开始）：
#   建 topic → 取消本实验旧作业 → 清空 source/sink topic → 提交 SQL → 等 RUNNING → 发消息 → 消费验证
# 用法: ./run.sh
set -euo pipefail
cd "$(dirname "$0")"

JOB_NAME='lab03-leftjoin-right-first'   # 与 demo.sql 里的 SET 'pipeline.name' 对应
ORDER_SRC_TOPIC='03_leftjoin_right_first_order_src'
PROPS_SRC_TOPIC='03_leftjoin_right_first_props_src'
REGULAR_SINK_TOPIC='03_leftjoin_right_first_regular_sink'
INTERVAL_SINK_TOPIC='03_leftjoin_right_first_interval_sink'
# 项目根的 ./lab 挂载到容器的 /opt/flink/lab
SQL_FILE="/opt/flink/lab/03_leftjoin_right_first/demo.sql"

echo '== 1/7 创建 topic（幂等）'
for T in "$ORDER_SRC_TOPIC" "$PROPS_SRC_TOPIC" "$REGULAR_SINK_TOPIC" "$INTERVAL_SINK_TOPIC"; do
  docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic "$T" --partitions 1 --replication-factor 1 2>/dev/null
done

echo '== 2/7 取消本实验的历史作业（按作业名匹配，不影响其他实验）'
CANCELLED=0
for jid in $(curl -s http://localhost:8081/jobs | grep -o '"id":"[a-f0-9]*"' | cut -d'"' -f4); do
  info=$(curl -s "http://localhost:8081/jobs/$jid")
  state=$(echo "$info" | grep -o '"state":"[A-Z]*"' | head -1 | cut -d'"' -f4)
  name=$(echo "$info" | grep -o '"name":"[^"]*"' | head -1 | cut -d'"' -f4)
  if [ "$state" = 'RUNNING' ] && [[ "$name" == *"$JOB_NAME"* ]]; then
    curl -s -X PATCH "http://localhost:8081/jobs/$jid?mode=cancel" >/dev/null
    echo "已取消历史作业 $jid ($name)"
    CANCELLED=1
  fi
done
[ "$CANCELLED" = 0 ] && echo '无历史作业' || sleep 3

echo '== 3/7 清空 source / sink topic'
../../scripts/purge-topic.sh "$ORDER_SRC_TOPIC" "$PROPS_SRC_TOPIC" "$REGULAR_SINK_TOPIC" "$INTERVAL_SINK_TOPIC"

echo '== 4/7 提交 SQL 作业'
SUBMIT_OUT=$(docker exec flink-jobmanager /opt/flink/bin/sql-client.sh -f "$SQL_FILE" 2>&1 || true)
echo "$SUBMIT_OUT" | grep -E 'Execute statement succeeded|ERROR|Job ID' || true
JOB_ID=$(echo "$SUBMIT_OUT" | grep -o 'Job ID: [a-f0-9]\+' | awk '{print $3}')
if [ -z "$JOB_ID" ]; then
  echo '未拿到 Job ID，提交失败？完整输出：' >&2
  echo "$SUBMIT_OUT" >&2
  exit 1
fi

echo '== 5/7 等待作业 RUNNING'
state=''
for _ in $(seq 1 30); do
  state=$(curl -s "http://localhost:8081/jobs/$JOB_ID" | grep -o '"state":"[A-Z]*"' | head -1 | cut -d'"' -f4 || true)
  [ "$state" = 'RUNNING' ] && break
  sleep 2
done
if [ "$state" != 'RUNNING' ]; then
  echo "作业 $JOB_ID 未进入 RUNNING（当前: ${state:-未知}），请到 http://localhost:8081 检查" >&2
  exit 1
fi
echo "作业 $JOB_ID 已 RUNNING"

echo '== 6/7 发送测试消息'
./send-messages.sh

echo '== 7/7 消费 sink topic（value 为 null 的是回撤墓碑）'
# 注意：macOS bash 3.2 下 $VAR 后紧跟全角字符会被误并入变量名，必须加 {} 隔开
echo "-- 普通 LEFT JOIN（${REGULAR_SINK_TOPIC}）"
../../scripts/consume.sh "$REGULAR_SINK_TOPIC"
echo "-- LEFT JOIN + 前后1min窗口（${INTERVAL_SINK_TOPIC}）"
../../scripts/consume.sh "$INTERVAL_SINK_TOPIC"
