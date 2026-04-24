# 2026-04-24 strategy snapshot 历史缓冲动态收口实现记录

## 本轮目标

- 把环境/因子的动态历史窗口继续接到 `StrategyService` 的 snapshot 起点解析上
- 同时避免这一步直接改掉当前默认参数下的用户可见行为

## 已落地内容

### 1. 新增 `strategy snapshot` 动态历史缓冲 helper

- 在 [shared/history_windows.py](C:/Users/ai/Desktop/可转债多因子/shared/history_windows.py) 新增：
  - `recommended_strategy_snapshot_history_buffer_calendar_days(config)`

当前规则是：

1. 先取：
   - `recommended_environment_history_buffer_calendar_days(config)`
   - `recommended_factor_history_buffer_calendar_days(config)`
2. 两者取最大值，作为 snapshot 最低所需历史缓冲
3. 再读取：
   - `strategy.history_buffer_calendar_days`
4. 当它大于 `0` 时：
   - 只负责把窗口放宽
5. 当它等于 `0` 时：
   - 直接进入自动模式

### 2. `resolve_strategy_snapshot_history_start(...)` 已统一复用新 helper

- [shared/history_windows.py](C:/Users/ai/Desktop/可转债多因子/shared/history_windows.py) 中的 `resolve_strategy_snapshot_history_start(...)` 现在不再自己拼固定最大值，而是统一改走新的 snapshot helper。

这样后续如果继续调整环境或因子的动态缓冲口径，`StrategyService` 会自动跟上，不需要再单独手改一处固定组合逻辑。

### 3. 参数文件已补充自动模式说明

- [策略参数.txt](C:/Users/ai/Desktop/可转债多因子/策略参数.txt) 中的 `strategy.history_buffer_calendar_days` 注释已更新。
- 现在明确说明：
  - `0` 表示按环境/因子窗口自动推导

本轮没有直接把默认值从 `550` 改成 `0`，所以当前用户默认行为仍保持保守。

## 当前行为结论

- `StrategyService` 的 snapshot 历史起点已经具备“动态需求 + 可选手工扩宽”的统一能力。
- 当前默认配置下，因为 `strategy.history_buffer_calendar_days = 550`，默认预览/观察行为不会突然变化。
- 如果后续要进入更激进的自动模式，只需要把：
  - `strategy.history_buffer_calendar_days = 0`

## 回归验证

已通过：

- `python -m unittest tests.test_history_windows tests.test_strategy_service tests.test_strategy_config -v`
- `python -m unittest tests.test_env -v`

新增或补强的回归重点包括：

- snapshot helper 支持 `strategy.history_buffer_calendar_days = 0` 自动模式
- 手工配置值不会把窗口压短到动态需求以下
- 现有 `StrategyService` 默认回归不受影响

## 下一步建议

- 下一轮更适合结合批量回测/批量预览日期调度，一起评估是否把默认 `550` 切到自动模式。
- 如果那一步确认稳定，再继续把更统一的预热窗口约束推进到回测主链。
