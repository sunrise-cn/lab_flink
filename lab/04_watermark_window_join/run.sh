#!/usr/bin/env bash
# 一键跑通本实验（每次从干净状态开始）：
#   建 topic → 取消本实验旧作业 → 清空 source/sink topic → 提交 SQL → 等 RUNNING
#   → 分 6 个阶段发消息，每阶段发完等 15s（> checkpoint 周期 10s）让 sink 落盘，然后消费观察
# 观察重点：每个阶段"sink 里有什么、没有什么"，体会水位线对输出的放行作用
# 用法: ./run.sh
set -euo pipefail
cd "$(dirname "$0")"

JOB_NAME='lab04-watermark-window-join'   # 与 demo.sql 里的 SET 'pipeline.name' 对应
ORDER_SRC_TOPIC='04_watermark_window_join_order_src'
PROPS_SRC_TOPIC='04_watermark_window_join_props_src'
WINDOW_SINK_TOPIC='04_watermark_window_join_window_sink'
JOIN_SINK_TOPIC='04_watermark_window_join_join_sink'
# 项目根的 ./lab 挂载到容器的 /opt/flink/lab
SQL_FILE="/opt/flink/lab/04_watermark_window_join/demo.sql"

echo '== 1/5 创建 topic（幂等）'
for T in "$ORDER_SRC_TOPIC" "$PROPS_SRC_TOPIC" "$WINDOW_SINK_TOPIC" "$JOIN_SINK_TOPIC"; do
  docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic "$T" --partitions 1 --replication-factor 1 2>/dev/null
done

echo '== 2/5 取消本实验的历史作业（按作业名匹配，不影响其他实验）'
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

echo '== 3/5 清空 source / sink topic'
../../scripts/purge-topic.sh "$ORDER_SRC_TOPIC" "$PROPS_SRC_TOPIC" "$WINDOW_SINK_TOPIC" "$JOIN_SINK_TOPIC"

echo '== 4/5 提交 SQL 作业'
SUBMIT_OUT=$(docker exec flink-jobmanager /opt/flink/bin/sql-client.sh -f "$SQL_FILE" 2>&1 || true)
echo "$SUBMIT_OUT" | grep -E 'Execute statement succeeded|ERROR|Job ID' || true
JOB_ID=$(echo "$SUBMIT_OUT" | grep -o 'Job ID: [a-f0-9]\+' | awk '{print $3}')
if [ -z "$JOB_ID" ]; then
  echo '未拿到 Job ID，提交失败？完整输出：' >&2
  echo "$SUBMIT_OUT" >&2
  exit 1
fi

echo '== 5/5 等待作业 RUNNING'
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

# 每个阶段：发消息 → 等 15s 让 sink 按 checkpoint 落盘 → 消费观察
# 注意：macOS bash 3.2 下 $VAR 后紧跟全角字符会被误并入变量名，必须加 {} 隔开
stage() {
  echo
  echo "########## 阶段 $1：$2"
  ./send-messages.sh "$1"
  echo '等待 15s 让 sink 完成 checkpoint flush ...'
  sleep 15
  echo "---- 此时 ${3} 的内容："
  ../../scripts/consume.sh "$3"
}

stage 1 '发窗口组订单 T001(10:00:10)、T002(10:00:40)，属同一窗口 [10:00,10:01)' "$WINDOW_SINK_TOPIC"
stage 2 '发 T003(10:02:00)，把水位线推过 10:01:00' "$WINDOW_SINK_TOPIC"
stage 3 '发 join 匹配组：属性 T101(10:10:20) 先到、订单 T101(10:10:50) 后到' "$JOIN_SINK_TOPIC"
stage 4 '发未匹配订单 T102(10:20:00)，并发 T103(10:22:00) 只推订单流水位线' "$JOIN_SINK_TOPIC"
stage 5 '发属性 T999(10:24:00)，推属性流水位线' "$JOIN_SINK_TOPIC"
stage 6 '发订单 T999(10:24:30) 收尾' "$JOIN_SINK_TOPIC"

echo
echo '########## 最终全量'
echo "---- 窗口分支（${WINDOW_SINK_TOPIC}）：注意 [10:24,10:25) 窗口永远不会出现"
../../scripts/consume.sh "$WINDOW_SINK_TOPIC"
echo "---- join 分支（${JOIN_SINK_TOPIC}）"
../../scripts/consume.sh "$JOIN_SINK_TOPIC"
