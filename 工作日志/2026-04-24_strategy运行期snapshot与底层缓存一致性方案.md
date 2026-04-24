# 2026-04-24 strategy 运行期 snapshot 与底层缓存一致性方案

## 1. 背景

当前 [StrategyService](C:/Users/ai/Desktop/可转债多因子/strategy/service.py) 已经支持：

- 同一实例内按 `trade_date + requested_history_start` 复用 runtime snapshot
- `refresh=True` 时旁路旧 snapshot，并在完成后回写新的 runtime snapshot

这层复用对“同日重复预览 / 调参重跑”已经有明显收益。

但它还缺一层边界保护：

- `StrategyService` 目前并不知道底层 `DataLoader / DataCacheService` 在同一进程内是否发生过写入

这会带来一个潜在问题：

- 如果第一次 build 之后，底层缓存或本地 reference 又被刷新 / writeback
- 第二次同日 build 仍然可能直接复用第一次的 runtime snapshot
- 从而绕过已经更新过的底层输入

## 2. 本轮目标

本轮只解决“同实例内 runtime snapshot 与底层输入一致性”的问题：

1. 底层依赖未变化时，保留当前 runtime snapshot reuse 收益。
2. 底层依赖一旦发生写入或刷新，旧 runtime snapshot 自动失效。
3. 不引入新的磁盘缓存。
4. 不做跨进程一致性治理。

## 3. 非目标

本轮不做：

- snapshot 跨进程缓存
- snapshot 按 dataset / month / trade_date 的细粒度部分失效
- reference metadata 的完整 schema/version 治理
- 批量回测层的 snapshot 复用调度

## 4. 问题边界

当前最值得处理的“输入变化”主要有两类：

### 4.1 `DataCacheService` 管辖下的写入

例如：

- `save_time_series`
- `save_static_frame`
- `save_time_series_aggregate`
- `save_time_series_aggregate_metadata`
- `save_time_series_coverage`

这些操作要么改变底层数据本体，要么改变高层缓存可用性。

### 4.2 `DataLoader` 直接触发的本地 reference 刷新

例如：

- `refresh_credit_spread_reference(...)`

它不走 `DataCacheService.save_*`，但会更新 `strategy snapshot` 依赖到的本地 reference 文件，因此同样应该让 runtime snapshot 失效。

## 5. 方案

### 5.1 在 `DataCacheService` 增加运行期内容修订号

新增一个只在当前进程内有效的整数计数器，例如：

- `runtime_content_generation`

每次发生写入时递增。

第一版可以直接把它定义为“保守失效信号”：

- 只要当前实例里发生过任何缓存写入，就认为旧 runtime snapshot 不再值得直接复用

这会比最细粒度失效稍保守，但行为安全、实现简单。

### 5.2 在 `DataLoader` 增加 runtime dependency revision

`DataLoader` 对 `StrategyService` 暴露一个轻量接口，例如：

- `runtime_dependency_revision()`

它至少包含两部分：

1. `DataCacheService` 的 `runtime_content_generation`
2. `DataLoader` 自己维护的 reference refresh generation

这样 `StrategyService` 不需要了解底层到底是 cache write 还是 reference refresh，只需要拿到一个“依赖是否变化”的修订号。

### 5.3 `StrategyService` 的 runtime snapshot 命中条件增加修订号匹配

当前 runtime snapshot memory 的 key 仍然保留：

- `trade_date`
- `requested_history_start`

但在取用 cached snapshot 时，额外比较：

- 当前 loader revision
- cached snapshot 保存时的 loader revision

只有两者一致时才允许复用。

如果不一致，则：

- 视为 stale runtime snapshot
- 重新走 build
- 用新 revision 覆盖回写 runtime snapshot memory

## 6. 设计判断

### 6.1 为什么不直接把 revision 塞进 memory key

可以，但没必要。

把 revision 作为单独校验值更清楚：

- 逻辑上仍然是“同一日期的一份 snapshot”
- revision 只是“这份 snapshot 还能不能继续拿来复用”的附加约束

### 6.2 为什么先做全局保守失效，而不是细粒度定向失效

因为当前 runtime snapshot reuse 的主要收益集中在：

- 同日重复运行、且中间没有新的写入

而真正有风险的场景是：

- 同一实例内发生写入后还继续吃旧 snapshot

先把这个风险收掉，比先做复杂的 dataset 级别失效更值。

## 7. 验证点

至少要补以下回归：

1. 同日二次运行、底层 revision 未变化：
   - 仍然命中 runtime snapshot reuse
2. 同日二次运行前，手动 bump 底层 revision：
   - 不能复用旧 snapshot
   - loader 主要取数调用次数应继续增加
3. `refresh=True`：
   - 仍然旁路旧 snapshot
   - 刷新后新的 snapshot 应按新 revision 写回

## 8. 完成后的下一步

如果这一层稳定，后续“缓存预热与失效机制”可以继续往两边推进：

1. 是否需要把细粒度 invalidation 再从“全局保守失效”缩窄到 dataset / month 级别
2. 是否需要给批量回测链路单独设计更长期限的 warmup / snapshot orchestration
