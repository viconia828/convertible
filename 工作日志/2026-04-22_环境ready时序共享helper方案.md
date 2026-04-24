# 2026-04-22 环境 ready 时序共享 helper 方案

## 1. 背景

截至 `2026-04-22`，项目已经把下面这些能力收口进共享 helper：

- 环境导出历史起点解析
- 因子导出历史起点解析
- `strategy` snapshot 历史窗口解析

但环境导出里仍有一组“ready 时序语义”留在 [scoring_exports.py](C:/Users/ai/Desktop/可转债多因子/exports/scoring_exports.py) 私有 helper 中：

- `warmup_first_ready_date`
- `trend_first_ready_date`
- `warmup_trade_days_excluded`
- 对应 warm-up 提示文本

这些语义虽然当前主要由环境导出使用，但本质上已经不再是 Excel 导出私有逻辑，而是“请求窗口如何转成正式可导出窗口”的共享时间口径。

因此，本轮继续把这部分从导出层抽成共享 helper，避免 `scoring_exports.py` 独占环境 ready 时序语义。

## 2. 目标

本轮目标：

1. 将环境导出 ready 时序解析收口到共享 helper。
2. 让 [scoring_exports.py](C:/Users/ai/Desktop/可转债多因子/exports/scoring_exports.py) 只消费解析结果，而不自己维护私有时间语义。
3. 为后续 `strategy` 或其他入口复用这套口径预留稳定接口。

## 3. 设计边界

本轮只抽共享 helper，不改变：

- 环境打分算法
- warm-up 观察数规则
- trend pre-ready 留空规则
- summary 字段含义
- 导出结果内容

也就是说，本轮是“语义收口”，不是“口径改版”。

## 4. 共享 helper 设计

建议在 [history_windows.py](C:/Users/ai/Desktop/可转债多因子/shared/history_windows.py) 中新增：

- `EnvironmentExportWindowResolution`
- `resolve_environment_export_window(...)`
- `resolve_environment_export_first_ready_date(...)`
- `first_ready_trade_date(...)`
- `build_environment_warmup_notes(...)`
- `count_trade_days_in_range(...)`

### 4.1 为什么要加 dataclass

如果只抽成多个零散函数，`scoring_exports.py` 仍然需要自己拼：

- first ready
- effective start
- excluded count
- warm-up notes

这样职责还是散的。

因此建议用一个轻量 dataclass 收口：

- `warmup_first_ready_date`
- `trend_first_ready_date`
- `effective_start`
- `warmup_trade_days_excluded`
- `notes`

这样调用方只需要消费一个结构化结果。

### 4.2 接口定位

这组 helper 仍然放在 `history_windows.py`，原因是它解决的本质问题仍然是：

- 一个“请求窗口”
- 如何根据 warm-up / readiness 规则
- 转成“实际可导出窗口”

虽然名字里带 `ready`，但它仍属于时间窗口解析语义，而不是导出文件格式语义。

## 5. 调用侧改造

在 [scoring_exports.py](C:/Users/ai/Desktop/可转债多因子/exports/scoring_exports.py) 中：

1. 用 `resolve_environment_export_window(...)` 替代现有多段私有 helper 调用。
2. 保留导出层自己的 report dataclass 与 summary 拼装。
3. 删除原本只在导出层内部使用的私有 helper，避免双份实现并存。

## 6. 测试要求

本轮至少补下面几类测试：

1. 共享 helper 直接测试：
   - first ready 日期解析
   - trend ready 日期解析
   - warm-up notes 生成
   - 交易日区间计数
2. 环境导出回归测试：
   - 预热充分时保留请求起点
   - 预热不足时自动跳过窗口内 warm-up 交易日
   - 整个请求窗口都处于 warm-up 时继续报错
   - trend pre-ready 区间继续留空

## 7. 结论

本轮最稳的做法是把“环境导出 ready 时序语义”继续收口到 [history_windows.py](C:/Users/ai/Desktop/可转债多因子/shared/history_windows.py)，而不是让 [scoring_exports.py](C:/Users/ai/Desktop/可转债多因子/exports/scoring_exports.py) 继续独占这组私有 helper。

这样做的收益是：

1. 环境导出主流程更短、更清晰。
2. ready 时序口径有了稳定共享入口。
3. 后续如果 `strategy` 或别的入口也要消费这套语义，不需要再从导出层复制一份。

