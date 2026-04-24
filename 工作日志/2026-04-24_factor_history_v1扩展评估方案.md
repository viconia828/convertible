# 2026-04-24 `factor_history_v1` 扩展评估方案

## 1. 背景

当前 `cb_daily_cross_section` 的聚合缓存，已经落地了一个稳定 profile：

- `factor_history_v1`

它现在已经服务两条主链：

- 因子打分导出 [scoring_exports.py](C:/Users/ai/Desktop/可转债多因子/exports/scoring_exports.py)
- `strategy` 单日期 snapshot / 预览 / 观察导出 [service.py](C:/Users/ai/Desktop/可转债多因子/strategy/service.py)

同时，项目最近两轮已经补齐了：

- `preview_strategy.py` 的本次运行缓存诊断
- `benchmark_factor_export.py` 的累计 + 逐轮迭代缓存诊断
- request panel cache / aggregate cache / runtime snapshot reuse 的分层观测

这意味着我们现在第一次具备了“不是凭感觉，而是凭诊断数据”来判断：

- `factor_history_v1` 是否已经覆盖了当前最热链路
- 是否还有第二个稳定、重复、值得单独做 aggregate profile 的请求模式

## 2. 本轮目标

这轮不是直接新增 profile，而是先回答三个问题：

1. 当前仓库里，除了 `factor_history_v1` 之外，是否真的存在第二个稳定高频的 `cb_daily_cross_section` 读取模式。
2. 如果存在，它是否已经大到足以影响后续 `strategy` 主链和回测阶段，而不是只在边角路径里偶发出现。
3. 如果不存在，是否应该明确接受“先不扩 profile”这个结论，而不是为了泛化而泛化。

## 3. 这轮评估为什么现在做

按当前项目路线，下一阶段迟早会进入：

- 更长历史窗口的 `strategy` 重复调用
- 独立回测模块
- baseline / 参数实验

而这几件事都会放大 `cb_daily_cross_section` 的读取成本。

所以，最合理的顺序不是“先写回测，再发现底层 profile 不够”，而是：

1. 先用现有单日 `strategy` 和 benchmark 入口看清热点
2. 判断是否需要补第二个稳定 profile
3. 再进入回测链路设计

## 4. 当前已知事实

### 4.1 `factor_history_v1` 已经覆盖的部分

当前 `factor_history_v1` 已经覆盖：

- 因子导出读取 `FactorEngine.HISTORY_COLUMNS`
- `StrategyService.build_snapshot(...)` 的历史横截面读取
- 对这条 profile 的月度聚合缓存
- 对这条 profile 的 request panel cache

也就是说：

- 因子打分和 `strategy` 单日期主链，当前本质上共用的是同一套 profile
- 对这条链路再额外起一个“名字不同、字段近似”的 profile，大概率只是重复建设

### 4.2 当前仓库里已知的另一条原始横截面调用链

除了上述链路外，当前明确还会直接调用 `get_cb_daily_cross_section(...)` 的，是：

- `cb_equal_weight` 指数重建 [data_loader.py](C:/Users/ai/Desktop/可转债多因子/data/data_loader.py)

而这条链路实际只依赖很少的原始列：

- `trade_date`
- `close`
- `pre_close`

这说明“第二候选”如果存在，当前最值得优先怀疑的不是 `strategy` 主链，而是：

- `cb_equal_weight` 重建链是否值得做一个更轻的 profile

## 5. 本轮评估边界

本轮纳入评估的对象：

- `cb_daily_cross_section` 的新 aggregate profile 是否值得加
- 新 profile 是否值得同步接 request panel cache
- 新 profile 是否应该优先服务 `cb_equal_weight` 或别的稳定上游请求

本轮不做：

- 直接实现新 profile
- 扩成“任意列自动建 profile”的通用系统
- 改动因子口径、环境口径或 `strategy` 选券逻辑
- 为导出结果、diagnostics sheet、watchlist sheet 这类派生结果做聚合缓存

## 6. 候选结论

这轮评估允许的结论不止一个。

### 6.1 结论 A：不新增 profile

这是完全有效的结论。

成立条件通常是：

- 当前热点几乎都已经被 `factor_history_v1` 覆盖
- `strategy` 主链上的额外收益主要来自 runtime snapshot reuse，而不是第二个原始 profile
- 其余路径请求频率太低，或列集合不稳定，不值得长期维护

### 6.2 结论 B：新增一个轻量 profile，优先服务 `cb_equal_weight` 重建

当前最具体的候选是：

- 暂定名：`cb_equal_weight_source_v1`

候选列集合原则上应只保留构建等权指数所需最小列，例如：

- `trade_date`
- `close`
- `pre_close`

如果后续标准化或按日分组路径要求保留额外识别列，再按真实调用需要补最小增量。

### 6.3 结论 C：新增第二个 profile，但不是现在就能预设命名

如果真实复核发现：

- 后续 `strategy` / 回测准备链上还有另一套稳定列投影
- 且它和 `FactorEngine.HISTORY_COLUMNS` 差异明确、复用频繁

那么可以在评估完成后再定具体 profile 名称，而不是现在凭想象先命名。

## 7. 评估问题清单

本轮至少要回答下面这些问题：

### 7.1 热点是否真的来自“第二套列投影”

要先区分：

- 是真的缺了第二个 aggregate profile
- 还是当前瓶颈其实已经转移到：
  - request panel 拼装
  - 环境层自身缓存覆盖
  - 回测尚未实现之前的上层复用不足

### 7.2 这个候选调用链是否足够稳定

只有满足下面条件，才适合被收口为 profile：

- 数据集固定：`cb_daily_cross_section`
- 列集合固定或低变动
- 请求窗口重复度高
- 不是一次性的调试代码路径

### 7.3 这个候选是否值得带上 request panel cache

当前 request panel cache 只对 `factor_history_v1` 启用。

如果新增 profile 之后：

- 它也会出现“同窗口、同进程、重复构建”的场景

那它不应该只拥有 aggregate cache，而应该一起纳入 request panel cache 的适用范围。

### 7.4 失效复杂度是否仍可控

如果新增 profile 需要引入：

- 独立写回规则
- 特殊失效链
- 单独 schema 兼容分支

那就说明它可能还没到值得独立维护的程度。

## 8. 评估方法

### 8.1 先用现有工具，不先加新工具

第一轮评估优先直接复用已有入口：

- `python tools/benchmark_factor_export.py`
- `python tools/preview_strategy.py --verbose`
- `python tools/export_strategy_observation.py`
- `python tools/export_environment_scores.py`

原则是：

- 先用已有诊断回答 80% 的问题
- 只有现有信息不足时，才补专门 benchmark

### 8.2 分三类样本观察

#### 样本 1：因子打分热链

目标：

- 确认 `factor_history_v1` 当前收益上限
- 作为后续“新增 profile 是否值得”的基线

建议口径：

- 半年窗口
- 少量代码
- 较多代码
- `repeat >= 2`

重点看：

- `iteration_build_seconds`
- `cache_diagnostics`
- `request_panel / aggregate / aggregate_metadata`
- `file_scan / remote_fill / writeback`

#### 样本 2：`strategy` 单日期主链

目标：

- 判断 `strategy` 当前是否还暴露出第二个原始横截面热点
- 分清收益到底来自 `factor_history_v1`，还是主要来自 runtime snapshot reuse

建议口径：

- 同一 `trade_date` 连跑
- 同一 `trade_date` + 不同观察名单
- 同一实例下复跑

重点看：

- `runtime snapshot reused`
- `request_panel`
- `aggregate`
- top stage timings

#### 样本 3：环境链 / `cb_equal_weight` 重建链

目标：

- 判断 `cb_equal_weight` 是否是第二个值得单独做 profile 的真实热点

建议口径：

- 较长环境窗口
- 首次运行与热缓存重跑对比
- 必要时单独观察 `cb_equal_weight` rebuild 时段

重点看：

- `cb_equal_weight` 的 `rebuild_from_cross_section`
- `cb_daily_cross_section` 是否在这条链上重复读了大量不必要列
- 现有 `cb_equal_weight` 自身缓存是否已经足够覆盖主要场景

## 9. 本轮重点指标

评估时优先看下面几组指标，而不是只看单次总耗时：

### 9.1 结构化缓存收益

- `request_panel`
- `aggregate`
- `aggregate_metadata`
- `runtime_snapshot_reused`

### 9.2 统一观测指标

- `hit / miss / partial hit / refresh bypass`
- `file_scan`
- `remote_fill`
- `writeback`

### 9.3 阶段耗时

至少关注：

- `request_panel_lookup`
- `aggregate_lookup`
- `day_cache_lookup`
- `aggregate_materialize`
- `result_concat`
- `rebuild_from_cross_section`

### 9.4 结果口径稳定性

任何候选 profile 都必须保证：

- 输出结果不变
- 只是读取优化，不改变业务语义

## 10. 通过门槛

只有同时满足下面几条，才建议进入“新增 profile 方案文档”阶段：

1. 候选调用链在主路径中真实存在，而不是测试或偶发路径。
2. 候选列集合稳定，不需要为少量特例频繁改 profile 定义。
3. 热缓存或重复运行收益明确，不只是统计命中成立但 wall-clock 没改善。
4. 收益不主要被现有 runtime snapshot reuse 或上层缓存替代。
5. 可复用现有粗粒度失效链，不需要引入明显更复杂的维护成本。

## 11. 否决条件

出现下面任一情况，就倾向于“不新增 profile”：

- 第二候选实际上只在低频链路里偶发出现
- 它与 `factor_history_v1` 的差异太小，只是换个名字重复维护
- 收益只体现在首次建缓存，不体现在后续稳定复用
- 命中统计成立，但 wall-clock 收益不稳定
- 需要额外的失效 / schema 特判，复杂度明显高于收益

## 12. 如果评估结论为“值得做”

下一步不直接写代码，而是再补一篇实施方案文档，至少明确：

- profile 名称
- 精确列集合
- 哪些入口接入
- 是否同步启用 request panel cache
- 失效链是否沿用现有粗粒度策略
- benchmark / regression 如何验证

## 13. 当前预判

在正式复核前，当前更偏向下面这个判断：

- `factor_history_v1` 仍然是当前主路径的核心 profile
- `strategy` 主链短期内大概率不需要再单独拆新 profile
- 最值得重点复核的第二候选，是 `cb_equal_weight` 重建链
- 但“最终不新增任何 profile”依然是高概率且可接受的结论

## 14. 推荐落地顺序

1. 先按本方案做一轮真实样本评估，不改代码。
2. 如果 `cb_equal_weight` 没有成为明确热点，则收口为“不新增 profile”。
3. 如果它成为明确热点，再落“新增轻量 profile”的实施方案文档。
4. 只有这一步收口后，再进入回测模块设计与实现。
