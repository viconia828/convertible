# 2026-04-24 `DataCacheService` 职责拆分方案

## 1. 背景

当前 [service.py](C:/Users/ai/Desktop/可转债多因子/data/cache/service.py) 已经膨胀到千行级别，并同时承接了多类职责：

- 统一缓存观测与计数
- 覆盖判断与历史起点探测
- 派生字段回写与高层缓存失效
- 列投影 / 标准化 / key 归一化一类策略函数
- request panel / aggregate frame / aggregate metadata 的运行期内存缓存
- calendar / static / time-series / aggregate / reference 的读写门面

虽然对外接口还算稳定，但内部已经越来越难继续演进：

- 观测逻辑和读写逻辑交错
- coverage / writeback / invalidation 相互穿插
- 纯函数 helper 和状态型方法混在一起
- 后续要补 schema / 口径版本治理时，很容易继续往单文件里堆

## 2. 本轮目标

这轮的目标不是重写缓存层，而是做一轮“低风险、保守”的职责拆分：

1. 保留 `DataCacheService` 作为统一门面，不改外部调用方式。
2. 先把最容易独立出来的职责拆成独立模块。
3. 第一阶段只做结构重排，不改缓存命中、失效和写回行为。

## 3. 拆分原则

### 3.1 先拆纯结构和纯职责，再拆核心状态

优先拆：

- 常量和类型别名
- 观测计数逻辑
- coverage / history inspect 逻辑
- 路径 / key / 列归一化策略
- writeback / invalidation 逻辑

暂时不拆：

- `DataCacheService.__init__`
- 三层运行期内存缓存本身的持有
- calendar / static / time-series / aggregate 的核心 load/save 门面

### 3.2 `DataCacheService` 继续做 facade

第一阶段不把上层接口改成“用户自己拼多个对象”，而是保持：

- `DataLoader`
- `TradingCalendar`
- `exports/*`
- `strategy/*`

继续只依赖一个 `DataCacheService`。

也就是说：

- 内部可以拆
- 对外还是一个门面

### 3.3 第一阶段优先“文件级拆分”，不是“对象图重写”

这一轮不引入复杂的依赖注入或服务注册，而是先把方法按职责迁到不同模块，再由 `DataCacheService` 统一组合。

这样做的好处是：

- 回归风险低
- 测试可直接复用
- 后续若要再把状态拆成独立对象，也有清晰落脚点

## 4. 目标模块结构

第一阶段目标结构：

```text
data/cache/
  __init__.py
  service.py
  observer.py
  coverage.py
  writeback.py
  policy.py
  models.py
```

各模块职责如下。

### 4.1 `models.py`

承接：

- 默认容量常量
- 运行期内存缓存 key 的类型别名
- 其他纯结构定义

### 4.2 `observer.py`

承接：

- `stats_snapshot`
- `observability_snapshot`
- `record_cache_resolution`
- `record_file_scan`
- `record_remote_fill`
- `record_writeback`
- `record_stage_timing`
- `_increment_panel_stat`
- `_increment_scoped_stat`
- `_increment_stat`

### 4.3 `coverage.py`

承接：

- `load_grouped_time_series`
- `covers_time_series`
- `covers_expected_dates`
- `covers_sparse_range`
- `load_time_series_coverage`
- `save_time_series_coverage`
- `load_time_series_aggregate_metadata`
- `save_time_series_aggregate_metadata`
- `covers_aggregate_trade_days`
- `inspect_local_env_history_start`
- `inspect_local_factor_history_start`

### 4.4 `policy.py`

承接：

- `time_series_coverage_path`
- `time_series_aggregate_metadata_path`
- `reference_frame_path`
- `reference_metadata_path`
- `_standardize_optional`
- `_normalize_requested_columns`
- `_safe_min_timestamp`
- `_aggregate_frame_memory_key`
- `_aggregate_metadata_memory_key`
- `_request_panel_memory_key`

### 4.5 `writeback.py`

承接：

- `writeback_derived_fields`
- `_invalidate_higher_level_caches_after_time_series_save`
- `invalidate_time_series_aggregate_month`
- `_invalidate_aggregate_memory_month`

## 5. 第一阶段实现方式

第一阶段采用 mixin / helper 组合方式：

- `DataCacheService` 继续持有状态
- 职责方法迁到独立模块中的 mixin
- `DataCacheService` 通过继承这些 mixin 重新组合

这样可以做到：

- 外部类名不变
- 外部接口不变
- 内部文件结构先拆开

## 6. 本轮不做的事

本轮不做：

- 不改 `DataLoader` 的调用方式
- 不改缓存目录结构
- 不新增 schema / 口径版本字段
- 不把内存缓存对象进一步抽成独立 manager
- 不改 request panel / aggregate / metadata 的命中与失效口径

## 7. 验证标准

完成后应满足：

1. `DataCacheService` 仍然对外保持同一套接口。
2. 现有 `tests/test_cache_service.py` 和 `tests/test_data_loader.py` 行为不回退。
3. `observer / coverage / writeback / policy / models` 五类职责在文件结构上已经独立落位。
4. `service.py` 不再同时承载所有纯 helper 和所有状态型方法。

## 8. 完成后的下一步

如果这轮结构拆分稳定，下一步更自然的是：

1. 为缓存层补 schema / 口径版本治理。
2. 再视情况把内存缓存状态进一步从 facade 中抽离。
