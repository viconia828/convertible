# 2026-04-24 strategy snapshot 历史缓冲动态收口方案

## 1. 背景

上一轮已经把环境导出的历史缓冲从固定长窗口，改成了：

- 按真实 `fully-ready` 需求动态推导
- 再受配置上限封顶

但 `StrategyService` 的 snapshot 历史起点仍然保留着旧式固定缓冲思路：

- `strategy.history_buffer_calendar_days`
- `exports.env_history_buffer_calendar_days`
- `recommended_factor_history_buffer_calendar_days(config)`

三者取最大值。

这会带来一个不对齐点：

- 环境导出已经是“动态需求 + cap”
- `strategy snapshot` 仍然容易被固定 `550` 天窗口直接兜住

## 2. 本轮目标

本轮目标不是马上把 `strategy` 默认行为切短，而是先把共享 helper 语义收口清楚：

1. `strategy snapshot` 也接入环境/因子的动态窗口推导
2. 保留手工扩宽 snapshot 历史窗口的能力
3. 不在这一轮悄悄改变当前默认配置下的用户可见行为

## 3. 决策

本轮采用“动态需求 + 可选手工扩宽”的折中方案：

- 先用环境动态窗口和因子动态窗口，推导出 `strategy snapshot` 的最低所需历史长度
- 再把 `strategy.history_buffer_calendar_days` 解释为“可选的手工扩宽值”
- 当它大于 `0` 时，只负责把窗口放宽，不允许把窗口压窄到动态需求以下
- 当它等于 `0` 时，表示使用自动模式，只按环境/因子动态需求决定 snapshot 起点

## 4. 本轮不做

本轮不做：

- 不把默认 `strategy.history_buffer_calendar_days` 从 `550` 直接改成 `0`
- 不改批量回测的日期调度口径
- 不改 `preview_strategy.py` / `export_strategy_observation.py` 的展示字段
- 不重写历史覆盖 warning 语义

## 5. 实现方式

建议在 [shared/history_windows.py](C:/Users/ai/Desktop/可转债多因子/shared/history_windows.py) 新增：

- `recommended_strategy_snapshot_history_buffer_calendar_days(config)`

规则：

1. 先取：
   - `recommended_environment_history_buffer_calendar_days(config)`
   - `recommended_factor_history_buffer_calendar_days(config)`
2. 两者取最大值，作为 snapshot 的最低所需历史缓冲
3. 再读取：
   - `strategy.history_buffer_calendar_days`
4. 若其大于 `0`，返回：
   - `max(dynamic_required, configured_buffer)`
5. 若其等于 `0`，返回：
   - `dynamic_required`

随后让：

- `resolve_strategy_snapshot_history_start(...)`

统一改走这层 helper。

## 6. 配置语义

这一轮把 `strategy.history_buffer_calendar_days` 的业务语义明确为：

- `0`：
  - 使用自动模式
- 正数：
  - 在动态需求基础上额外保守扩宽

这意味着它不再适合被理解为“必须固定取这么长”，而更接近：

- “strategy snapshot 的手工保守缓冲下限”

## 7. 验证点

至少覆盖三类测试：

1. `strategy.history_buffer_calendar_days = 0` 时，snapshot helper 走自动模式
2. `strategy.history_buffer_calendar_days` 小于动态需求时，不允许把窗口压短
3. 当前默认配置仍保持原有保守行为，不误伤现有 `StrategyService` 回归

## 8. 完成后的下一步

如果这一步稳定，下一轮再决定：

1. 是否结合批量回测日期调度，把默认 `strategy.history_buffer_calendar_days` 切到 `0`
2. 是否继续把预热窗口约束往批量 preview / rebalance / backtest 主链推进
