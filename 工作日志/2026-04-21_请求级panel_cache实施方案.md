# 2026-04-21 请求级 panel cache 实施方案

## 1. 目标

基于 [2026-04-21_请求级panel_cache评估.md](C:/Users/ai/Desktop/可转债多因子/工作日志/2026-04-21_请求级panel_cache评估.md)，本轮实现一个第一版可落地、可观察、风险可控的请求级 `panel cache`。

本轮目标只有三件事：

1. 在统一缓存层里增加 runtime-only request panel cache
2. 补上 `cb_daily_cross_section` 写回到更高层缓存的粗粒度失效链
3. 让真实样本 benchmark 能直接看到 panel cache 的 hit / miss / invalidation

## 2. 范围

### 2.1 本轮纳入

- `cb_daily_cross_section`
- `standardized_name = "cb_daily"`
- `aggregate_profile = "factor_history_v1"`
- 运行期内存缓存
- 同进程内重复请求复用

### 2.2 本轮不纳入

- 磁盘级 panel cache
- 没有 `aggregate_profile` 的通用横截面请求
- 其他 dataset 的请求级 panel cache
- 精细到单个 trade day 的局部 panel 失效

## 3. 核心设计

### 3.1 缓存位置

请求级 panel cache 放在 [service.py](C:/Users/ai/Desktop/可转债多因子/data/cache/service.py) 的 `DataCacheService` 内部。

原因：

- 缓存策略继续统一收口在缓存层
- 统计、失效、容量限制都能集中管理
- `DataLoader` 只负责给出请求 key 和最终结果

### 3.2 缓存 key

第一版 key 由以下字段组成：

- `dataset_name`
- `standardized_name`
- `aggregate_profile`
- `projected_columns`
- `first_trade_day`
- `last_trade_day`
- `trade_day_count`
- `trade_day_digest`

其中：

- `projected_columns` 使用已归一化后的列集合
- `trade_day_digest` 使用完整 `trade_day_strs` 生成稳定摘要，避免不同交易日集合误命中

### 3.3 缓存 value

缓存 value 是 `get_cb_daily_cross_section(...)` 在当前请求口径下的最终结果，也就是：

- 已完成标准化
- 已完成投影
- 已完成按请求交易日过滤
- 已完成跨月拼接

的最终 panel。

### 3.4 返回对象策略

和现有聚合分片运行期缓存一致，本轮 panel cache 返回共享只读对象口径，不做整表深拷贝。

原因：

- 这层缓存的目标就是避免重复装配成本
- 如果每次命中都整表深拷贝，收益会被明显稀释

当前因子链路下这一点可接受，因为：

- [FactorEngine._select_history_columns](C:/Users/ai/Desktop/可转债多因子/factor/factor_engine.py:299)
  会先对输入 `cb_daily` 做 `.copy()`

## 4. 接入点

在 [data_loader.py](C:/Users/ai/Desktop/可转债多因子/data/data_loader.py) 的 `get_cb_daily_cross_section(...)` 中：

1. 先拿到 `trade_day_strs`
2. 若 `refresh=False` 且 `aggregate_profile` 非空，则先查 request panel cache
3. 命中则直接返回最终 panel
4. 未命中再走现有：
   - 月度聚合缓存
   - 按日缓存
   - 远端补数
5. 在最终 `pd.concat(...)` 后把结果写入 request panel cache

## 5. 失效策略

### 5.1 为什么这轮必须做失效

当前 [writeback_derived_fields](C:/Users/ai/Desktop/可转债多因子/data/cache/service.py:479)
会把 `ytm` 等派生值回写到按日 `cb_daily_cross_section` 缓存。

如果这时：

- 月度聚合缓存不失效
- 请求级 panel cache 不失效

那么后续读取仍可能命中旧月分片或旧 panel。

### 5.2 第一版失效规则

采用“粗粒度但安全”的版本：

- `save_time_series(dataset_name="cb_daily_cross_section", cache_key=YYYYMMDD)`
  - 失效该 dataset 的所有请求级 panel cache
  - 失效该月份下的所有聚合缓存分片：
    - 删除对应 profile 的 `YYYYMM.csv`
    - 删除对应 profile 的 `YYYYMM.meta.json`
  - 清理对应月份的聚合 frame / metadata 运行期内存缓存

- `save_time_series_aggregate(dataset_name="cb_daily_cross_section", profile=...)`
  - 失效该 dataset / profile 的所有请求级 panel cache

- `save_time_series_aggregate_metadata(dataset_name="cb_daily_cross_section", profile=...)`
  - 失效该 dataset / profile 的所有请求级 panel cache

- `refresh=True`
  - 直接 bypass request panel cache

## 6. 观测指标

在 `DataCacheService.stats_snapshot()` 中新增：

- `panel_memory_hit_calls`
- `panel_memory_miss_calls`
- `panel_memory_save_calls`
- `panel_memory_invalidation_calls`
- `aggregate_partition_invalidation_calls`

并保留 dataset / profile 级细分统计。

## 7. 测试计划

### 7.1 cache service 直接测试

新增测试覆盖：

- request panel cache 首次 miss、二次 hit
- `save_time_series("cb_daily_cross_section", ...)` 会失效：
  - request panel cache
  - 对应月份聚合缓存文件
  - 对应月份聚合运行期内存缓存

### 7.2 data loader 接入测试

新增测试覆盖：

- `get_cb_daily_cross_section(...)` 能复用 request panel cache
- 按日写回 `ytm` 后，再次读取不会命中旧 aggregate / 旧 panel

## 8. 验证标准

完成后应满足：

1. 同进程内重复请求同一 `cb_daily_cross_section + factor_history_v1` 窗口时，出现明确 `panel_memory_hit_calls`
2. `persist_cb_daily_cross_section_derived_fields(...)` 后，再次读取不会返回旧 `ytm`
3. 受影响月份的聚合缓存文件会被粗粒度失效
4. 回归测试继续通过
5. benchmark 能输出 panel cache 相关统计

## 9. 本轮实施结果

本轮已经完成实现并验证：

- [data/cache/service.py](C:/Users/ai/Desktop/可转债多因子/data/cache/service.py)
  - 新增 runtime-only request panel cache
  - 新增 `panel_memory_hit/miss/save/invalidation` 统计
  - 新增 `aggregate_partition_invalidation_calls`
  - 在 `save_time_series("cb_daily_cross_section", ...)` 上补了更高层缓存失效链：
    - 失效 request panel cache
    - 删除受影响月份的聚合缓存文件
    - 清理受影响月份的聚合运行期内存缓存
- [data/data_loader.py](C:/Users/ai/Desktop/可转债多因子/data/data_loader.py)
  - 在 `get_cb_daily_cross_section(...)` 中接入 request panel cache 的 hit/save
  - 第一版只对 `aggregate_profile="factor_history_v1"` 启用
- [tests/test_cache_service.py](C:/Users/ai/Desktop/可转债多因子/tests/test_cache_service.py)
  - 新增 request panel cache 命中测试
  - 新增按日写回触发的聚合缓存 / request panel cache 失效测试
- [tests/test_data_loader.py](C:/Users/ai/Desktop/可转债多因子/tests/test_data_loader.py)
  - 新增 request panel cache 复用测试
  - 新增 `persist_cb_daily_cross_section_derived_fields(...)` 后不再读取旧 aggregate / 旧 panel 的测试

回归结果：

- `python -m unittest tests.test_cache_service tests.test_data_loader tests.test_scoring_exports tests.test_factor tests.test_derived_metrics -v`
- `49` 个测试全部通过

真实样本复核：

- benchmark 口径：
  - `2025-10-01 ~ 2026-04-17`
  - `113048.SH` / `113039.SH`
  - `--skip-write --repeat 2`
- 结果显示：
  - 已出现 `panel_memory_hit_calls = 1`
  - 已出现 `panel_memory_save_calls = 1`

同进程 A/B 对照：

- panel disabled:
  - `timings = [4.775, 4.017]`
- panel enabled:
  - `timings = [3.960, 3.160]`

当前判断：

- request panel cache 已经开始给同进程重复构建带来更直接的收益
- 相比只缓存聚合分片，它更有效地绕开了“逐月命中 + 过滤 + 拼接”的重复装配成本
- 后续如果继续优化，更值得评估：
  - 是否扩展到更多稳定 profile
  - 是否在 `strategy` 主链路里复用这层 panel cache
