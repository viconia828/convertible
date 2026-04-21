# 2026-04-21 请求级 panel cache 评估

## 1. 背景

在已经完成：

- `cb_rate` 按缺失 `ytm` 代码按需加载
- `cb_daily_cross_section` 按列读取
- `factor_history_v1` 月度聚合缓存
- 聚合缓存运行期内存缓存

之后，`cb_daily_cross_section` 的读取成本已经从“按日小文件”逐步压缩到“按月聚合分片 + 少量运行期复用”。

但真实样本复核显示：

- 聚合分片级运行期内存缓存的 hit 行为已经成立
- wall-clock 收益仍不稳定

这说明当前剩余成本不只是“分片读盘”，还包括：

- 多个月分片的逐月 metadata / frame 命中判断
- 命中后逐月过滤、拼接
- 同一请求在同一进程内重复构建时，重复做相同的面板装配

因此，下一步更值得评估的是“请求级 panel cache”。

## 2. 当前热点观察

### 2.1 真实样本请求规模

按当前因子打分链路的真实请求口径：

- 请求窗口：`2025-10-01 ~ 2026-04-17`
- 实际历史起点：`2025-06-01`
- 列口径：`FactorEngine.HISTORY_COLUMNS`
- `aggregate_profile = "factor_history_v1"`

测得当前 `cb_daily` 面板规模约为：

- `88625` 行
- `214` 个交易日
- `515` 只转债
- `8` 列
- 内存占用约 `9.128 MB`

结论：

- 这个量级做“运行期请求级内存缓存”是可接受的
- 即使缓存 `2 ~ 4` 个同量级 panel，请求级缓存的额外内存也大致仍在几十 MB 范围

### 2.2 当前聚合分片层的边界

当前同进程重复构建时，虽然已经可以命中：

- `aggregate_memory_hit_calls`
- `aggregate_metadata_memory_hit_calls`

但仍需要：

- 逐月判断覆盖
- 逐月取分片
- 逐月筛交易日
- 最后再拼成一次完整 panel

也就是说，当前缓存层已经缓存了“原料分片”，但还没有缓存“最终请求结果”。

## 3. panel cache 应该放在哪里

### 3.1 放在 DataLoader 内部

优点：

- 接入简单
- 最靠近 `get_cb_daily_cross_section(...)` 的装配逻辑

缺点：

- 会把新的缓存策略重新放回业务协调层
- 与“缓存策略统一收口到缓存层”的目标相冲突

结论：

- 不推荐作为最终方案

### 3.2 放在 DataCacheService，作为运行期请求级缓存

优点：

- 仍然符合“缓存层统一管理策略”的方向
- 可以统一统计 hit / miss / invalidation
- 后续若扩展到环境层或别的 profile，也能复用同一套机制

缺点：

- `DataLoader` 仍需负责告诉缓存层“这次请求的 key 是什么”
- 需要设计清楚失效策略

结论：

- 推荐作为第一版方案

### 3.3 做成磁盘级 panel cache

优点：

- 跨进程可复用

缺点：

- 失效复杂度远高于运行期缓存
- 与当前日级缓存、月度聚合缓存容易形成三层重复物化
- 会明显放大 schema / 口径 / 回写一致性问题

结论：

- 当前阶段不推荐

## 4. 推荐方案

### 4.1 范围

第一版只做：

- 运行期内存缓存
- `cb_daily_cross_section`
- 因子打分链路专用请求
- `aggregate_profile = "factor_history_v1"`

不做：

- 磁盘 panel cache
- 全数据集通用面板缓存
- 环境层和因子层所有请求的统一一把梭

### 4.2 缓存 key

建议 key 至少包含：

- `dataset_name`
- `standardized_name`
- `aggregate_profile`
- `projected_columns`
- `trade_day_strs` 的稳定摘要

其中：

- `trade_day_strs` 不建议直接整串拼成长 key
- 更稳的是使用：
  - `first_trade_day`
  - `last_trade_day`
  - `trade_day_count`
  - `trade_day_digest`

### 4.3 缓存 value

缓存 value 为：

- 已完成标准化
- 已完成投影
- 已完成按请求交易日过滤
- 已完成跨月份拼接

的最终 `cb_daily` panel。

也就是说，它缓存的是：

- `get_cb_daily_cross_section(...)`

在当前请求口径下的最终结果，而不是中间分片。

### 4.4 命中点

推荐在 [data_loader.py](C:/Users/ai/Desktop/可转债多因子/data/data_loader.py) 的 `get_cb_daily_cross_section(...)` 中：

1. 先完成交易日标准化和请求 key 计算
2. 先询问缓存层是否已有 panel hit
3. 命中则直接返回最终 panel
4. 未命中再继续走当前：
   - 月度聚合缓存
   - 按日缓存
   - 远端补数
5. 最终拼出 panel 后再写回请求级 panel cache

这样，业务装配逻辑仍在 `DataLoader`，但“缓存策略”仍集中在缓存层。

## 5. 失效策略是第一前提

这是这轮评估里最重要的结论之一。

当前代码里：

- [writeback_derived_fields](C:/Users/ai/Desktop/可转债多因子/data/cache/service.py:479)
  会通过 [save_time_series](C:/Users/ai/Desktop/可转债多因子/data/cache/service.py:107) 回写 `cb_daily_cross_section`
- 但这条链路目前不会自动失效更上层的聚合缓存或请求级 panel cache

这意味着：

- 如果直接叠加 panel cache，而不先补失效策略
- 那么旧值会被更高层缓存放大

因此第一版 panel cache 的失效策略建议采取“粗粒度但安全”的方案：

- 任何 `save_time_series(dataset_name="cb_daily_cross_section", ...)`
  - 清空该 dataset 对应的请求级 panel cache
- 任何 `save_time_series_aggregate(dataset_name="cb_daily_cross_section", ...)`
  - 清空该 dataset / profile 对应的请求级 panel cache
- `refresh=True`
  - 直接 bypass 请求级 panel cache

结论：

- 运行期缓存 + 粗粒度失效，比“复杂精细失效”更适合第一版

## 6. 为什么它比“分片内存缓存”更值得继续试

请求级 panel cache 的潜在收益，比当前分片级缓存更直接：

- 不再逐月命中 metadata / frame
- 不再逐月筛交易日
- 不再逐月拼接
- 不再重复构建同一个 `cb_daily` panel

换句话说：

- 分片缓存缓存的是“构件”
- panel cache 缓存的是“最终请求结果”

如果用户在同一进程内反复：

- 导出同一窗口
- 微调参数后重跑
- 后续在 `strategy` 主链路中重复使用同一历史 panel

那么 panel cache 更有机会给出稳定收益。

## 7. 风险与注意事项

### 7.1 旧值放大风险

如果失效链不补，panel cache 会比月度聚合缓存更容易扩大旧值问题。

### 7.2 共享对象的可变性

为了保留性能收益，运行期 panel cache 更适合返回共享对象而不是整表深拷贝。

这要求上层调用链满足“只读消费”。

当前因子链路里，[FactorEngine._select_history_columns](C:/Users/ai/Desktop/可转债多因子/factor/factor_engine.py:299)
会先对输入 `cb_daily` 做 `.copy()`，因此对现有因子链路是相对安全的。

但如果后续别的调用链直接修改返回 panel，就需要额外约束。

### 7.3 首次运行无收益

请求级 panel cache 只对同进程重复请求有效，对首次请求没有直接收益。

## 8. 评估结论

结论很明确：

- 请求级 panel cache 值得做
- 第一版应做成“统一缓存层内的运行期内存缓存”
- 第一版只服务 `cb_daily_cross_section + factor_history_v1 + 因子打分链路`
- 但实现前必须把“写回 / 聚合更新 -> panel cache 失效”这条链补清楚

## 9. 推荐落地顺序

1. 先在 `DataCacheService` 增加请求级 panel cache 能力与统计
2. 在 `get_cb_daily_cross_section(...)` 中接入 panel hit / save
3. 在 `save_time_series(...)` / `save_time_series_aggregate(...)` 上加粗粒度失效
4. 补 benchmark 指标：
   - `panel_memory_hit_calls`
   - `panel_memory_miss_calls`
   - `panel_memory_invalidation_calls`
5. 用真实样本复核：
   - 同窗口 repeat
   - 相同历史窗口、不同请求代码集合

如果这轮仍不能拿到稳定收益，再考虑更高一级的：

- 因子准备阶段 snapshot cache
- 或直接在 `strategy` 层做更完整的请求结果复用
