# 2026-04-22 strategy snapshot 运行期复用方案

## 1. 背景

前一轮已经完成：

- `cb_daily_cross_section + factor_history_v1` 月度聚合缓存
- 统一缓存层内的 runtime-only request panel cache

当前 `strategy` 主链路在 [StrategyService](C:/Users/ai/Desktop/可转债多因子/strategy/service.py) 中，已经能间接受益于这层 panel cache，因为 `build_snapshot(...)` 会走：

- [DataLoader.get_cb_daily_cross_section(...)](C:/Users/ai/Desktop/可转债多因子/data/data_loader.py)
- `aggregate_profile="factor_history_v1"`

但如果同一进程内连续多次构建同一个 `trade_date` 的策略预览，`StrategyService` 仍然会重复装配：

- trading calendar
- macro daily
- cb_daily history panel
- cb_basic
- cb_call
- cb_rate
- history notes

也就是说，request panel cache 的收益虽然已经存在，但还没有以更直接的方式传导到 `strategy` 入口。

## 2. 目标

本轮目标不是再做一层新的磁盘缓存，而是补一层短生命周期、同实例内的 `strategy snapshot` 运行期复用，让“同一交易日重复预览 / 调参重跑”能更直接复用已装配好的 snapshot。

目标包括：

1. 同一个 `StrategyService` 实例内，重复构建同一个 `trade_date` 时，不重复走整套数据装配。
2. `requested_codes` 继续只影响预览展示，不参与 snapshot 复用键。
3. `refresh=True` 时不命中旧 snapshot，但本次刷新完成后应覆盖写回新的运行期 snapshot。
4. 不新增新的磁盘缓存文件，不让 `strategy` 直接操作缓存文件细节。

## 3. 非目标

本轮不做：

- 新的磁盘级 `strategy snapshot cache`
- 跨进程 snapshot 复用
- 批量预览 / 回测层的 snapshot 复用
- request panel cache 观测指标统一报表
- `factor_history_v1` 之外更多 aggregate profile

## 4. 设计判断

### 4.1 复用层放在哪里

底层 panel cache 仍然继续留在 [DataCacheService](C:/Users/ai/Desktop/可转债多因子/data/cache/service.py)。

但“把多份已取回数据装配成一个 `StrategySnapshot`”这一步，本身属于 `StrategyService` 的职责。因此更直接的做法是：

- 继续让 `DataLoader / DataCacheService` 管数据缓存
- 在 `StrategyService` 内补一层只管已装配 snapshot 的运行期 memoization

这样可以避免让 `strategy` 去碰缓存文件，同时又把收益往上层推一格。

### 4.2 snapshot 复用键

第一版建议按以下维度命中：

- `trade_date`
- `requested_history_start`

这层缓存是 `StrategyService` 实例私有的，因此：

- 不需要再把 config 全量展开进 key
- `requested_codes` 不进入 key

原因是 `requested_codes` 只影响 diagnostics / 预览展示，不影响全市场环境分数、因子分数和选券口径。

### 4.3 refresh 语义

- `refresh=False`
  - 允许命中已有 runtime snapshot
- `refresh=True`
  - 不命中旧 runtime snapshot
  - 重新走数据装配
  - 但完成后要把新的 snapshot 写回 runtime snapshot memory

这样可以避免“刷新后同实例仍返回旧 snapshot”。

### 4.4 共享对象与只读约束

和 request panel cache 一样，这层复用默认返回共享 DataFrame 引用，不做整表深拷贝。

前提是上层调用链继续满足“只读消费”：

- [StrategyEngine](C:/Users/ai/Desktop/可转债多因子/strategy/engine.py) 不直接改写 snapshot 内的原始 frame
- 因子计算链路内部若需要改列，继续在自己的工作副本上进行

## 5. 实现草图

### 5.1 `strategy/snapshot.py`

在 `StrategySnapshot` 上补一个轻量标记：

- `runtime_snapshot_reused: bool = False`

只表达“这次返回是否命中了 `StrategyService` 的运行期 snapshot 复用”。

### 5.2 `strategy/service.py`

新增：

- 私有 LRU 容器 `_snapshot_memory`
- 容量上限 `snapshot_memory_items`
- `_load_runtime_snapshot(...)`
- `_save_runtime_snapshot(...)`
- `_snapshot_memory_key(...)`

`build_snapshot(...)` 逻辑调整为：

1. 先规范化 `trade_date` 和 `requested_codes`
2. 解析 `requested_history_start`
3. 若 `refresh=False`，优先查 runtime snapshot
4. 命中则直接返回：
   - 复用原始 frame
   - 仅覆写 `requested_codes`
   - 标记 `runtime_snapshot_reused=True`
5. 未命中则继续按当前逻辑构建完整 snapshot
6. 新建完成后把“可缓存版本”写入 runtime snapshot memory
7. 返回给调用方的版本保留本次 `refresh_requested` 和 `requested_codes`

### 5.3 `strategy/result.py` / `strategy/engine.py`

把 `runtime_snapshot_reused` 透传进 diagnostics，便于后续预览入口或缓存观测继续消费，但本轮不强制新增终端展示。

## 6. 测试计划

在 [tests/test_strategy_service.py](C:/Users/ai/Desktop/可转债多因子/tests/test_strategy_service.py) 补三类覆盖：

1. 同实例、同 `trade_date`、`refresh=False` 的二次构建：
   - 第二次命中 runtime snapshot
   - 不重复调用 loader 的主要取数方法
2. `requested_codes` 不参与复用键：
   - 第二次输入不同观察名单
   - 返回的 `requested_codes` 应更新
   - 但底层 frame 继续复用
3. `refresh=True` 旁路已有 runtime snapshot：
   - 本次重新走 loader
   - 返回 `runtime_snapshot_reused=False`
   - 刷新后的结果重新写回 runtime snapshot memory

## 7. 验收标准

1. `StrategyService` 同实例重复构建同日 snapshot 时，第二次不再重复装配主要输入 frame。
2. `requested_codes` 的展示语义不变，不污染 snapshot 复用。
3. `refresh=True` 不会误命中旧 snapshot，且刷新后同实例继续可复用新值。
4. 现有 `strategy` 子集回归继续通过。
