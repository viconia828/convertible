# 2026-04-21 cb_daily_cross_section 缓存读取优化方案

## 1. 背景

在统一缓存层和 `cb_rate` 按需加载优化之后，真实样本 benchmark 显示新的主要热点已经集中到 `cb_daily_cross_section` 的按日缓存读取。

当前因子打分链路会调用：

- [scoring_exports.py](C:/Users/ai/Desktop/可转债多因子/exports/scoring_exports.py)
- [data/data_loader.py](C:/Users/ai/Desktop/可转债多因子/data/data_loader.py)

去读取请求窗口内所有交易日的 `cb_daily_cross_section` 缓存文件。

## 2. 已确认的问题

当前 `cb_daily_cross_section` 的缓存文件虽然已经支持批量拼接，但仍然存在两个浪费：

1. 每个按日 CSV 都会完整读取所有列
2. 因子打分实际只使用少量字段，却要为每个交易日解析完整横截面

对因子打分链路而言，真正会用到的主要字段只有：

- `cb_code`
- `trade_date`
- `close`
- `amount`
- `premium_rate`
- `ytm`
- `convert_value`
- `is_tradable`

也就是 [factor/factor_engine.py](C:/Users/ai/Desktop/可转债多因子/factor/factor_engine.py) 中 `HISTORY_COLUMNS` 对应的最小历史输入。

## 3. 本轮优化目标

本轮目标是降低 `cb_daily_cross_section` 的本地按日读盘成本，而不改变缓存文件格式和业务计算口径。

具体目标：

- 为缓存读取链路增加按列读取能力
- 因子打分读取 `cb_daily_cross_section` 时仅加载最小必要列
- 保持缓存文件仍然按原格式完整写入
- 保持 `cb_equal_weight_index` 等依赖完整列的场景不受影响

## 4. 设计方案

### 4.1 在底层缓存读取增加列投影

在 [data/cache_store.py](C:/Users/ai/Desktop/可转债多因子/data/cache_store.py) 中：

- 为 `load_time_series(...)` 增加可选 `columns` 参数
- 底层 `read_csv` 改为支持 `usecols`
- 使用 `callable usecols` 形式，保证即使旧缓存文件缺少某些列也不会直接报错

这样可以在不改缓存文件格式的前提下，减少按日 CSV 的解析列数。

### 4.2 在缓存层批量读取透传列投影

在 [data/cache/service.py](C:/Users/ai/Desktop/可转债多因子/data/cache/service.py) 中：

- `load_time_series(...)` 增加 `columns` 参数
- `load_grouped_time_series(...)` 增加 `columns` 参数
- 批量读取时将列投影直接透传到底层缓存读取

### 4.3 在 DataLoader 的 `cb_daily_cross_section` 上层接口增加按列读取

在 [data/data_loader.py](C:/Users/ai/Desktop/可转债多因子/data/data_loader.py) 中：

- 为 `get_cb_daily_cross_section(...)` 增加可选 `columns` 参数
- 默认仍为 `None`，表示完整返回，保证兼容现有调用方
- 当调用方传入列集合时：
  - 缓存命中路径只读所需列
  - 远端补数后仍然完整写缓存，但返回给上层前只保留所需列

### 4.4 因子打分链路改为按最小必要列读取

在 [scoring_exports.py](C:/Users/ai/Desktop/可转债多因子/exports/scoring_exports.py) 中：

- 调用 `get_cb_daily_cross_section(...)` 时传入 `FactorEngine.HISTORY_COLUMNS`
- 只为因子打分链路加载必要列

## 5. `ytm` 回写兼容策略

当前因子打分链路在估算出缺失 `ytm` 后，会调用：

- `persist_cb_daily_cross_section_derived_fields(...)`

之前曾通过 `base_frame` 复用已读取的 `cb_daily` 面板，但在引入按列读取后，这个 `base_frame` 将不再保证包含完整缓存列。

因此本轮同步调整策略：

- 因子打分链路不再向回写函数传入精简后的 `base_frame`
- 派生值回写时，按实际受影响交易日重新读取完整缓存文件再合并保存

这样可以保证：

- 不会用精简列面板覆盖掉完整缓存文件
- 列投影优化和派生回写逻辑彼此独立

## 6. 实施范围

本轮只做：

- `cb_daily_cross_section` 的按列读取
- 因子打分链路的最小列加载
- `ytm` 回写路径兼容调整
- 相关测试和 benchmark

本轮不做：

- 缓存格式升级为聚合文件
- `parquet` 改造
- 运行期内存缓存
- 独立 derived cache

## 7. 验证标准

完成后应满足：

1. 因子打分结果口径不变
2. `cb_daily_cross_section` 在因子打分链路只读取最小必要列
3. `ytm` 回写不会覆盖掉原缓存中的其他列
4. 回归测试继续通过
5. 真实样本 benchmark 的纯构建耗时继续下降

## 8. 本轮实施结果

本轮已经完成实现并验证：

- [data/cache_store.py](C:/Users/ai/Desktop/可转债多因子/data/cache_store.py)
  - 为 CSV 读取增加可选列投影
  - 优先走列表式 `usecols`，仅在列不匹配时才回退
- [data/cache/service.py](C:/Users/ai/Desktop/可转债多因子/data/cache/service.py)
  - 为 `load_time_series(...)` / `load_grouped_time_series(...)` 增加 `columns` 透传
  - 自动补齐 schema key columns 和 group column
- [data/data_loader.py](C:/Users/ai/Desktop/可转债多因子/data/data_loader.py)
  - 为 `get_cb_daily_cross_section(...)` 增加 `columns` 参数
  - 因缓存分片已标准化，拼接后不再重复做整段 `standardize`
- [scoring_exports.py](C:/Users/ai/Desktop/可转债多因子/exports/scoring_exports.py)
  - 因子打分链路按 `FactorEngine.HISTORY_COLUMNS` 读取 `cb_daily_cross_section`
  - `ytm` 回写不再复用精简列面板

回归结果：

- `python -m unittest tests.test_cache_service tests.test_data_loader tests.test_scoring_exports tests.test_factor tests.test_derived_metrics -v`
- `40` 个测试全部通过

真实样本 benchmark：

- 对比口径：两只转债、`2025-10-01 ~ 2026-04-17`、`--skip-write`
- 上一轮基线：纯构建约 `6.8s`
- 本轮优化后：纯构建约 `5.5s`

结论：

- `cb_daily_cross_section` 按列读取这条优化已经确认有效
- 当前阶段继续往下压性能，最值得评估的是：
  - 是否将 `cb_daily_cross_section` 从按日 CSV 升级为聚合缓存
  - 是否在单次运行内引入轻量内存缓存，减少重复读盘

