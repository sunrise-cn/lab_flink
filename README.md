# Flink SQL 学习平台

本机 docker compose 一键启动 Kafka + Flink，按"实验（lab）"隔离验证 Flink SQL 特性。平台组成、运维命令、实验规范和踩坑记录见 `AGENTS.md`。

## 统一实验背景：订单表 × 订单属性表

为了让所有实验共享同一套业务语境、降低理解负担，本项目全部实验基于同一个贴近真实的背景：**保险/服务类订单平台**的订单域（字段命名沿用了公司真实平台的叫法）。

业务流程：用户提交服务申请后生成**订单**；订单的行业属性（如服务生效时间）由外部供应商系统**异步回填**。由此产生两条业务流：

**订单流（order）** —— 订单主事件。一单一条，但上游重试、数据修正会让同一订单号出现多条不同版本的消息。

| 字段 | 含义 |
|:--|:--|
| `za_order_no` | 订单号（join key） |
| `apply_t` | 申请（下单）时间 |

**订单属性流（order_props）** —— 订单的行业属性。天然异步于订单流（属性晚到），且会被多次修正（同一 `za_order_no` 先后出现多个版本）。

| 字段 | 含义 |
|:--|:--|
| `za_order_no` | 订单号（join key） |
| `hyfw_start` | 行业服务开始时间 |

这套背景天然覆盖流式 join 的典型问题：**双侧重复**（重试 / 修正版本）、**异步晚到**（属性回填滞后）、**回撤更新**（属性修正覆盖旧值）。

## 实验列表

| 实验 | 验证点 |
|:--|:--|
| `lab/01_leftjoin_and_inner_join` | LEFT JOIN vs INNER JOIN 的输出差异（null 补齐行、回撤墓碑） |
| `lab/02_join_duplicate_messages` | 双侧同 key 重复消息的 join：笛卡尔积放大，LEFT/INNER 一致 |
| `lab/03_leftjoin_right_first` | LEFT JOIN 右流先到左流后到：普通 join 永久可匹配，interval join（±1min）超窗不匹配且 null 行延迟输出 |
| `lab/04_watermark_window_join` | 水位线的作用：窗口结果等水位线关门、join 匹配不等水位线、未匹配 null 行等水位线、双输入取最小值 |

各实验的目的、复现命令和实测结论见对应目录下的 `README.md`。
