#!/usr/bin/env python3
"""实验 04 全过程时间线图：8 条消息的事件时间、水位线推进与 sink 输出时机。

运行：python3 timeline.py  →  在同目录生成 timeline.png（README.md 引用）

图分上下两个面板，共用同一条事件时间轴（2026-08-13 10:00 起算）：
  ① 上方面板：对角线 y=x 上的点是消息本身（订单▲ / 属性▼）；
     阶梯线是水位线（= 已见最大事件时间 - 5s），join 算子水位线 = 两侧最小值。
  ② 下方面板：两条 sink 分支的输出时机——空心圈 = 数据已到但输出被憋住，
     实心点 = 结果落盘，虚线弧 = 被水位线憋住的时长。
"""
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

plt.rcParams['font.sans-serif'] = ['PingFang SC', 'Hiragino Sans GB', 'Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False

C_ORDER, C_PROPS, C_OP, C_WIN = '#1565C0', '#E65100', '#2E7D32', '#C62828'


def t(minute, sec=0):
    """10:00:00 起算的秒数。"""
    return minute * 60 + sec


def fmt(sec):
    return f"10:{sec // 60:02d}:{sec % 60:02d}"


# ---------------- 实验数据（与 send-messages.sh 一致） ----------------
# (阶段, key, 事件时间)
ORDERS = [(1, 'T001', t(0, 10)), (1, 'T002', t(0, 40)), (2, 'T003', t(2)),
          (3, 'T101', t(10, 50)), (4, 'T102', t(20)), (4, 'T103', t(22)),
          (6, 'T999', t(24, 30))]
PROPS = [(3, 'T101', t(10, 20)), (5, 'T999', t(24))]
STAGES = [(1, t(0, 10)), (2, t(2)), (3, t(10, 20)), (4, t(20)), (5, t(24)), (6, t(24, 30))]

# 窗口分支：1 分钟滚动窗口及其计数（最后一个是 T999 订单所在窗口，永远等不到关门）
WINDOWS = [(t(0), t(1), 2), (t(2), t(3), 1), (t(10), t(11), 1),
           (t(20), t(21), 1), (t(22), t(23), 1), (t(24), t(25), 1)]
# join 分支未匹配订单（无属性）：右边界 = apply_ts + 1min
UNMATCHED = [('T001', t(0, 10)), ('T002', t(0, 40)), ('T003', t(2)),
             ('T102', t(20)), ('T103', t(22))]
MATCHED = [('T101', t(10, 50)), ('T999', t(24, 30))]  # 匹配成功，立即输出

WM = 5  # 水位线 = 已见最大事件时间 - 5s
X_END = t(27)

# ---------------- 推进水位线（复算，和引擎行为一致） ----------------
events = sorted([(x, 'o') for _, _, x in ORDERS] + [(x, 'p') for _, _, x in PROPS])
ow_x, ow_y, pw_x, pw_y, op_x, op_y = [], [], [], [], [], []
ow = pw = None
for x, kind in events:
    if kind == 'o':
        ow = x - WM
        ow_x, ow_y = ow_x + [x], ow_y + [ow]
    else:
        pw = x - WM
        pw_x, pw_y = pw_x + [x], pw_y + [pw]
    if ow is not None and pw is not None:          # 双输入算子水位线 = min(两侧)
        op_x, op_y = op_x + [x], op_y + [min(ow, pw)]


def first_cross(xs, ys, threshold):
    """水位线阶梯线首次越过 threshold 的 x（即结果落盘的触发消息）。"""
    return next((x for x, y in zip(xs, ys) if y > threshold), None)


# 窗口关门时刻：订单流水位线 > 窗口结束时间
win_emit = [(s, e, c, first_cross(ow_x, ow_y, e)) for s, e, c in WINDOWS]
# null 补齐行放行时刻：算子水位线 > 订单的窗口右边界（apply_ts + 1min）
null_release = [(k, x, first_cross(op_x, op_y, x + t(1))) for k, x in UNMATCHED]

# ---------------- 画图 ----------------
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 9.5), sharex=True,
                               gridspec_kw={'height_ratios': [3, 2], 'hspace': 0.08})
fig.suptitle('实验 04 全过程时间线：水位线如何控制输出时机', fontsize=14)

for ax in (ax1, ax2):                                   # 阶段分隔线
    for stage, x in STAGES:
        ax.axvline(x, color='#bbbbbb', ls='--', lw=0.8, zorder=0)
        if ax is ax1:
            if stage == 5:      # 右上角太挤：阶段5 下移
                ax.text(x + 8, 0.78, f'阶段{stage}', transform=ax.get_xaxis_transform(),
                        fontsize=9, color='#555555', va='top')
            elif stage == 6:    # 阶段6 移到线左侧，避开 T999 订单标签
                ax.text(x - 8, 0.985, f'阶段{stage}', transform=ax.get_xaxis_transform(),
                        fontsize=9, color='#555555', va='top', ha='right')
            else:
                ax.text(x + 8, 0.985, f'阶段{stage}', transform=ax.get_xaxis_transform(),
                        fontsize=9, color='#555555', va='top')

# ===== 面板①：消息与水位线 =====
ax1.plot([-30, X_END], [-30, X_END], ls=':', color='#999999', lw=1,
        label='对角线 y=x（事件时间 = 数据本身的时间）')
# 算子水位线 = min(两侧)，几乎总贴着某一条流走：画成粗半透明底带衬在最底层，
# 两条流加粗画在上面（属性流用虚线），重合处也能分辨出三条线
ax1.step(op_x + [X_END], op_y + [op_y[-1]], where='post', color=C_OP, lw=7, alpha=0.35,
         label='join 算子水位线 = min(两侧)')
ax1.step(ow_x + [X_END], ow_y + [ow_y[-1]], where='post', color=C_ORDER, lw=2.6,
         label='订单流水位线（最大事件时间 - 5s）')
ax1.step(pw_x + [X_END], pw_y + [pw_y[-1]], where='post', color=C_PROPS, lw=2.6,
         linestyle=(0, (5, 2)), label='属性流水位线（最大事件时间 - 5s）')

for _, k, x in ORDERS:                                  # 订单事件（对角线上）
    dy = 26 if k in ('T002', 'T103') else 10            # 与邻居错开
    if k == 'T999':                                     # 右上角太挤，挪到点的右侧
        ax1.annotate(f'{k} {fmt(x)}', (x, x), textcoords='offset points', xytext=(14, 4),
                     ha='left', fontsize=7.5, color=C_ORDER)
    else:
        ax1.annotate(f'{k} {fmt(x)}', (x, x), textcoords='offset points', xytext=(0, dy),
                     ha='center', fontsize=7.5, color=C_ORDER)
    ax1.scatter([x], [x], marker='^', s=55, color=C_ORDER, zorder=5, edgecolor='white')
for _, k, x in PROPS:                                   # 属性事件
    if k == 'T999':                                     # 右上角太挤，挪到点的右下方空区
        ax1.annotate(f'{k} {fmt(x)}', (x, x), textcoords='offset points', xytext=(10, -40),
                     ha='left', fontsize=7.5, color=C_PROPS)
    else:
        ax1.annotate(f'{k} {fmt(x)}', (x, x), textcoords='offset points', xytext=(0, -18),
                     ha='center', fontsize=7.5, color=C_PROPS)
    ax1.scatter([x], [x], marker='v', s=55, color=C_PROPS, zorder=5, edgecolor='white')

ax1.scatter([], [], marker='^', s=55, color=C_ORDER, label='订单消息')
ax1.scatter([], [], marker='v', s=55, color=C_PROPS, label='属性消息')

# 关键门槛与越过点
ax1.hlines(t(1), -25, t(2), colors=C_WIN, ls=':', lw=1.2)
ax1.text(t(2) + 15, t(1) + 35, '窗口 [10:00,10:01) 结束时间 10:01:00', fontsize=7.5, color=C_WIN)
ax1.scatter([t(2)], [t(1)], marker='*', s=180, color=C_WIN, zorder=6)
ax1.text(t(2) + 30, -t(0, 45), '订单流水位线 > 10:01:00\n→ 窗口关门输出', fontsize=8, color=C_WIN)
ax1.hlines(t(21), t(20), t(24), colors=C_OP, ls=':', lw=1.2)
ax1.scatter([t(24)], [t(21)], marker='*', s=180, color=C_OP, zorder=6)
ax1.annotate('算子水位线 > 10:21:00\n→ T102 null 行放行', (t(24), t(21)),
             xytext=(t(15, 30), t(18, 30)), fontsize=8, color='darkgreen',
             arrowprops=dict(arrowstyle='->', color='darkgreen', lw=0.8))
ax1.hlines(t(23), t(22), t(24, 30), colors=C_OP, ls=':', lw=1.2)
ax1.scatter([t(24, 30)], [t(23)], marker='*', s=180, color=C_OP, zorder=6)
ax1.annotate('算子水位线 > 10:23:00\n→ T103 null 行放行', (t(24, 30), t(23)),
             xytext=(t(15), t(22, 30)), fontsize=8, color='darkgreen',
             arrowprops=dict(arrowstyle='->', color='darkgreen', lw=0.8))
ax1.text(t(21, 30), t(8, 50), '阶段4：订单流水位线已推到 10:21:55，\n但算子水位线卡在 10:10:15（取两侧最小值）',
         fontsize=8.5, color='darkgreen', ha='center',
         bbox=dict(fc='white', ec=C_OP, lw=0.8, alpha=0.9))

ax1.set_ylim(-260, t(26, 30))
ax1.set_ylabel('时间值（水位线 / 事件）', fontsize=10)
ax1.set_title('① 消息（对角线上的点）与水位线（阶梯线）：只有被后续消息"推一把"才前进',
              fontsize=11, loc='left')
ax1.legend(loc='upper left', fontsize=8, framealpha=0.95)
ax1.grid(alpha=0.25)

# ===== 面板②：sink 输出时机 =====
Y_NULL, Y_MATCH, Y_WIN = 1, 2, 3
for s, e, c, emit in win_emit:                          # 窗口条 + 落盘点
    never = emit is None
    ax2.broken_barh([(s, e - s)], (Y_WIN - 0.13, 0.26),
                    facecolors='#f4b6b6' if never else '#9ecae1',
                    edgecolor=C_WIN if never else '#3182bd', lw=0.8)
    ax2.text((s + e) / 2, Y_WIN + 0.22, f'[{fmt(s)[:6]},{fmt(e)[:6]})×{c}', ha='center',
             fontsize=7, color=C_WIN if never else '#3182bd')
    if never:
        ax2.text(e + 15, Y_WIN, '永远缺席：之后没有消息\n把水位线推过 10:25:00',
                 fontsize=8, color=C_WIN, va='center')
    else:
        ax2.scatter([emit], [Y_WIN], s=70, color=C_OP, zorder=5)
        ax2.annotate('', (emit, Y_WIN - 0.2), (e, Y_WIN - 0.2),
                     arrowprops=dict(arrowstyle='->', color='#3182bd', lw=0.9, ls=':',
                                     connectionstyle='arc3,rad=0.2'))

for k, x in MATCHED:                                    # join 匹配行：立即输出
    ax2.scatter([x], [Y_MATCH], marker='s', s=65, color=C_OP, zorder=5)
    ax2.text(x, Y_MATCH + 0.18, f'{k} 匹配成功\n立即输出（不等水位线）',
             ha='center', fontsize=8, color='darkgreen')

for k, x, rel in null_release:                          # join null 补齐行：等水位线
    ax2.scatter([x], [Y_NULL], s=55, facecolor='white', edgecolor='#777777', zorder=5)
    ax2.annotate('', (rel, Y_NULL + 0.06), (x, Y_NULL + 0.06),
                 arrowprops=dict(arrowstyle='->', color='#999999', lw=0.9, ls=':',
                                 connectionstyle='arc3,rad=0.28'))
    ax2.scatter([rel], [Y_NULL], s=65, color=C_WIN, zorder=6)
for keys, rel, dy in [('T001/T002', t(10, 20), -0.42), ('T003', t(10, 50), -0.75),
                      ('T102', t(24), -0.42), ('T103', t(24, 30), -0.75)]:
    ax2.annotate(f'{keys} EMPTY 放行', (rel, Y_NULL), xytext=(rel, Y_NULL + dy),
                 ha='center', fontsize=8, color=C_WIN,
                 arrowprops=dict(arrowstyle='->', color=C_WIN, lw=0.7))

ax2.set_ylim(0.1, 3.75)
ax2.set_yticks([Y_NULL, Y_MATCH, Y_WIN])
ax2.set_yticklabels(['join_sink\n未匹配 null 补齐行', 'join_sink\n匹配行', 'window_sink\n窗口结果'],
                    fontsize=9)
ax2.set_xlim(-30, X_END)
ax2.set_xticks(range(0, X_END + 1, t(2)))
ax2.set_xticklabels([f'10:{x // 60:02d}' for x in range(0, X_END + 1, t(2))])
ax2.set_xlabel('事件时间（2026-08-13）', fontsize=10)
ax2.set_title('② sink 输出时机：空心圈 = 数据已到但输出被憋住，实心点 = 落盘，虚线弧 = 憋了多久',
              fontsize=11, loc='left')
ax2.legend(handles=[
    Line2D([], [], marker='o', ls='', mfc='white', mec='#777777', label='数据到达，输出被憋住'),
    Line2D([], [], marker='o', ls='', mfc=C_WIN, mec=C_WIN, label='join null 补齐行落盘'),
    Line2D([], [], marker='s', ls='', mfc=C_OP, mec=C_OP, label='join 匹配行（立即输出）'),
    Line2D([], [], marker='o', ls='', mfc=C_OP, mec=C_OP, label='窗口结果落盘'),
], loc='center left', bbox_to_anchor=(0.005, 0.62), fontsize=8, framealpha=0.95)
ax2.grid(alpha=0.25)

out = Path(__file__).with_name('timeline.png')
fig.savefig(out, dpi=150, bbox_inches='tight')
print(f'已生成 {out}')
