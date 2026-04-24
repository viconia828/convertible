# 2026-04-21 strategy notes / summary 共享口径方案

## 1. 背景

截至 `2026-04-21`，`strategy` 已经具备：

- 第一阶段骨架
- 单日期预览入口
- 与导出层共享的历史窗口 helper

但在“运行结果语义”上，`strategy` 和导出层仍然不是同一套口径。

当前导出层已经稳定使用：

- `fetch_policy`
- `refresh_requested`
- `data_quality_status`
- 数据质量 warning note
- 对齐摘要

对应代码主要在：

- [scoring_exports.py](C:/Users/ai/Desktop/可转债多因子/exports/scoring_exports.py)

而 `strategy` 当前更多是零散的：

- `history_start_requested / used`
- `data_quality_hints`
- `notes`
- `alignment_summary`

对应代码主要在：

- [strategy/result.py](C:/Users/ai/Desktop/可转债多因子/strategy/result.py)
- [strategy/engine.py](C:/Users/ai/Desktop/可转债多因子/strategy/engine.py)
- [tools/preview_strategy.py](C:/Users/ai/Desktop/可转债多因子/tools/preview_strategy.py)

这样的问题是：

1. `strategy` 预览入口无法直接显示和导出层一致的“数据质量状态”。
2. 数据质量 warning note 目前仍然由各自模块维护，容易继续分叉。
3. `preview` 终端展示和导出层 summary 的概念没有完全对齐。

## 2. 目标

本轮目标不是让 `strategy` 复制 Excel summary，而是让它复用导出层已经稳定下来的“报告语义”。

也就是共享：

- 取数策略口径
- 刷新请求口径
- 数据质量状态口径
- 数据质量 warning 文案
- 对齐摘要文本口径

## 3. 本轮范围

本轮只做：

- 新增一个共享 reporting semantics helper
- 让导出层和 `strategy` 共同复用这些基础语义
- 在 `StrategyDiagnostics` 中补齐：
  - `fetch_policy`
  - `refresh_requested`
  - `data_quality_status`
- 让 `preview_strategy.py` 直接展示这些字段

## 4. 本轮不做什么

本轮不纳入：

- 导出层 Excel summary 重构
- `StrategyDiagnostics` 变成完整 Excel-style summary table
- 环境 / 因子 diagnostics 结构大改
- 回测层接入

## 5. 建议模块位置

建议新增：

- [reporting_semantics.py](C:/Users/ai/Desktop/可转债多因子/shared/reporting_semantics.py)

原因：

- 它服务的是导出层和 `strategy` 层的共同语义
- 不属于单一业务模块
- 与 [history_windows.py](C:/Users/ai/Desktop/可转债多因子/shared/history_windows.py) 的定位相似，都是跨层共享 helper

## 6. 建议共享的能力

### 6.1 固定语义常量

统一收口：

- 默认 `fetch_policy`
- `data_quality_status` 的“正常 / 警告”

避免这几个中文状态值继续在多个模块硬编码。

### 6.2 布尔标签

统一收口：

- `是 / 否`

当前导出层在 summary 里已经使用，`strategy` 预览也可以直接复用。

### 6.3 数据质量状态判定

建议提供：

- `resolve_data_quality_status(has_issue: bool)`

让导出层和 `strategy` 层都用同一套状态映射，而不是各自手写 `"警告" if ... else "正常"`。

### 6.4 数据质量 warning 文案

建议提供：

- `build_data_quality_warning_note(context: str)`

当前这句 warning 文案在导出层已经稳定，`strategy` 应直接复用，而不是再写一个近似版本。

### 6.5 对齐摘要文本

建议提供：

- `format_alignment_summary(summary)`

这样终端预览和后续其他入口都能复用同一套：

- `calendar=...`
- `kept=...`
- `dropped=...`

## 7. strategy 侧建议改动

### 7.1 StrategySnapshot

建议补：

- `refresh_requested`

因为 `StrategyEngine` 生成 diagnostics 时需要知道当前运行是否显式请求了刷新。

### 7.2 StrategyDiagnostics

建议补：

- `fetch_policy`
- `refresh_requested`
- `data_quality_status`

保留现有：

- `history_start_requested / used`
- `data_quality_hints`
- `notes`
- `alignment_summary`

### 7.3 StrategyEngine

建议：

- 不再把 `snapshot.data_quality_hints` 直接复制进 `notes`
- `data_quality_hints` 保持只承接历史覆盖类提示
- `notes` 承接：
  - fully-ready 不足提示
  - 组合构建提示
  - 数据质量 warning note

并由：

- `snapshot.data_quality_hints`
- fully-ready 是否不足

共同决定 `data_quality_status`

## 8. 导出层建议改动

导出层不改业务行为，只做语义复用：

- `fetch_policy` 默认值改为共享常量
- `data_quality_status` 判定改为共享 helper
- warning note 改为共享 helper
- `refresh_requested` 的“是/否”标签改为共享 helper

## 9. preview 入口改动

建议在 [preview_strategy.py](C:/Users/ai/Desktop/可转债多因子/tools/preview_strategy.py) 中的“基本信息”区补充：

- `取数策略`
- `刷新请求`
- `数据质量状态`

这样 `strategy` 预览和导出层在最关键的运行语义上就对齐了。

## 10. 测试要求

建议新增或更新：

- `tests/test_reporting_semantics.py`
- `tests/test_strategy_engine.py`
- `tests/test_strategy_preview_tool.py`

至少覆盖：

1. 数据质量状态 helper 的正常 / 警告映射。
2. `StrategyEngine` 在 history 不足或 fully-ready 不足时打出 `警告`。
3. `preview` 输出包含：
   - `取数策略`
   - `刷新请求`
   - `数据质量状态`

## 11. 结论

本轮最稳的做法不是让 `strategy` 去复制导出层的 Excel summary，而是先共享“报告语义”。

这样既能让 `strategy` 预览更像项目现有的正式入口，也能避免导出层和 `strategy` 后续在 warning / status / hint 这些高频文案上继续分叉。

