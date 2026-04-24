# 2026-04-24 strategy 运行期 snapshot 与底层缓存一致性实现记录

## 本轮目标

- 保留 `StrategyService` 的同实例 runtime snapshot reuse 收益。
- 但只要同一进程里底层输入发生写入或 reference 刷新，就不再继续复用旧 snapshot。
- 范围只限运行期内存层，不新增新的磁盘缓存。

## 已落地内容

### 1. `DataCacheService` 增加运行期内容修订号

- 在 [data/cache/service.py](C:/Users/ai/Desktop/可转债多因子/data/cache/service.py) 增加：
  - `runtime_content_generation()`
  - `mark_runtime_content_mutation()`
- 当前这些写路径都会推进 generation：
  - `save_calendar`
  - `save_static_frame`
  - `save_time_series`
  - `save_time_series_coverage`
  - `save_time_series_aggregate`
  - `save_time_series_aggregate_metadata`

这版把它定义成“保守失效信号”：

- 只要同实例里发生过缓存写入，就认为旧 runtime snapshot 不再值得直接复用。

### 2. `DataLoader` 增加 runtime dependency revision

- 在 [data/data_loader.py](C:/Users/ai/Desktop/可转债多因子/data/data_loader.py) 新增 `runtime_dependency_revision()`。
- 当前 revision 由两部分组成：
  - `DataCacheService.runtime_content_generation()`
  - `credit_spread` reference refresh generation

这样 `StrategyService` 不需要知道底层具体发生了哪类写入，只需要判断依赖 revision 是否变化。

### 3. `credit_spread` reference 刷新也纳入失效边界

- `refresh_credit_spread_reference(...)` 成功返回后，会推进 loader 自己的 reference generation。
- 因此即使这类刷新不是通过 `DataCacheService.save_*` 写入，后续同日 snapshot 也不会继续误复用旧值。

### 4. `StrategyService` 现在按 revision 校验 runtime snapshot

- [strategy/service.py](C:/Users/ai/Desktop/可转债多因子/strategy/service.py) 中：
  - runtime snapshot memory 仍然按 `trade_date + requested_history_start` 组织
  - 但命中前会额外比较当前 `runtime_dependency_revision`
- 只有 revision 一致时才允许直接复用。
- 如果 revision 不一致：
  - 旧 runtime snapshot 会被当作 stale 旁路
  - 本次重新 build
  - 然后按新的 revision 覆盖回写

## 当前行为结论

- 同日重复预览、且中间没有写入：仍然能命中 runtime snapshot reuse。
- 同日重复预览前，如果：
  - 做过缓存 writeback
  - 做过 refresh
  - 做过 `credit_spread` reference 刷新

旧 runtime snapshot 都不会再被继续复用。

- 这版是“全局保守失效”，不是 dataset 级精细失效。
  - 优点是安全、实现简单
  - 代价是同一实例里只要发生任意写入，旧 snapshot 都会失效一次

## 回归验证

已通过：

- `python -m unittest tests.test_cache_service -v`
- `python -m unittest tests.test_strategy_service -v`
- `python -m unittest tests.test_data_loader tests.test_strategy_engine -v`
- `python -m unittest tests.test_scoring_exports tests.test_strategy_preview_tool -v`

新增或补强的回归重点包括：

- `DataCacheService` 写路径会推进 runtime content generation
- `StrategyService` 在 loader revision 变化后，不再继续复用旧 runtime snapshot

## 下一步建议

- 接下来更适合继续收口“预热窗口约束”和“批量回测前的 cache invalidation 边界”。
- 如果后续发现“全局保守失效”影响了同实例命中率，再考虑把 revision 从全局级缩窄到 dataset / month 级别。
