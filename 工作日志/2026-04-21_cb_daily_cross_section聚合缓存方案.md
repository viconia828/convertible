# 2026-04-21 cb_daily_cross_section 聚合缓存方案

## 1. 背景

在完成：

- `cb_rate` 按缺失 `ytm` 代码按需加载
- `cb_daily_cross_section` 按列读取

之后，真实样本 benchmark 已经把因子打分纯构建耗时压到约 `5.5s`。但缓存统计仍显示主要成本来自：

- `cb_daily_cross_section` 的按日小文件读取

即使每个文件只读少量列，窗口跨半年时仍要打开两百多个按日 CSV。

## 2. 本轮优化目标

本轮不直接做“通用全量聚合缓存重构”，而是先做一个更稳、更聚焦的版本：

- 为 `cb_daily_cross_section` 增加“投影型月度聚合缓存”
- 只优先服务当前最热的因子打分链路

这样可以：

- 复用现有按日缓存作为底层真值
- 不改现有远端取数和日级缓存写入逻辑
- 先把最热链路从“读 200 多个小文件”变成“读少数几个按月文件”

## 3. 设计原则

### 3.1 日级缓存仍然是主缓存

当前按日缓存仍作为 canonical source：

- 远端取数仍写回按日缓存
- 派生值回写仍写回按日缓存

月度聚合缓存只作为读取优化层，不替代日级缓存。

### 3.2 聚合缓存按“投影 profile”区分

本轮不做“全列月度缓存”，而是做“投影型月度缓存”。

原因：

- 当前真正的热点是因子打分使用的最小列集合
- 如果强行维护全列月度缓存，会引入更多同步和回写复杂度
- 投影型缓存可以直接复用当前按列读取结果构建，不需要再额外重读完整日文件

本轮先定义一个 profile：

- `factor_history_v1`

其字段口径对应：

- `FactorEngine.HISTORY_COLUMNS`

## 4. 目标结构

在缓存目录下新增聚合缓存层级，例如：

```text
data/cache/tushare/time_series_aggregate/
  cb_daily_cross_section/
    factor_history_v1/
      202510.csv
      202510.meta.json
      202511.csv
      202511.meta.json
```

说明：

- `202510.csv` 表示 2025 年 10 月的月度聚合缓存
- 同目录 metadata 记录该月已经覆盖的交易日集合

## 5. 数据内容与 metadata

### 5.1 月度聚合 CSV

CSV 中保存该 profile 对应的投影列，例如：

- `cb_code`
- `trade_date`
- `close`
- `amount`
- `premium_rate`
- `ytm`
- `convert_value`
- `is_tradable`

### 5.2 metadata

metadata 至少记录：

- `covered_trade_days`
- `projection_columns`

其中 `covered_trade_days` 用于判断该月缓存是否足以覆盖本次请求窗口。

## 6. 读路径

在 [data/data_loader.py](C:/Users/ai/Desktop/可转债多因子/data/data_loader.py) 的 `get_cb_daily_cross_section(...)` 中增加可选聚合读取逻辑：

1. 如果调用方传入 `aggregate_profile`
2. 先按月拆分请求交易日
3. 逐月尝试读取聚合缓存
4. 只有当该月 metadata 覆盖了本次请求的全部交易日时，才直接命中聚合缓存
5. 未命中的月份继续走现有按日缓存读取逻辑

## 7. 写路径

本轮不在远端取数路径直接维护月度缓存，而是在读取后按需物化：

1. 当某个月没有命中月度聚合缓存时
2. 当前请求仍会通过日级缓存拿到该月所需的逐日结果
3. 如果这个月的请求交易日都已经成功取得，就把这些结果顺手写成该 profile 的月度聚合缓存

这样带来的行为是：

- 第一次跑：主要作用是“顺手建立月度聚合缓存”
- 第二次跑同窗口或重叠窗口：可以显著减少小文件读取次数

## 8. 对因子打分链路的接入方式

在 [scoring_exports.py](C:/Users/ai/Desktop/可转债多因子/scoring_exports.py) 中：

- `build_factor_score_report(...)` 调用 `get_cb_daily_cross_section(...)` 时：
  - 继续传 `columns=list(engine.HISTORY_COLUMNS)`
  - 新增传 `aggregate_profile="factor_history_v1"`

这样因子打分链路会优先使用其专属的投影型月度聚合缓存。

## 9. 本轮不做的内容

本轮不做：

- 全列月度聚合缓存
- 聚合缓存反向回写到日级缓存
- `parquet` 改造
- 通用任意 profile 的策略系统

## 10. 验证标准

完成后应满足：

1. 第一次运行可以自动建立 `factor_history_v1` 月度聚合缓存
2. 第二次运行同窗口或重叠窗口时，小文件读取次数明显下降
3. 因子打分结果口径不变
4. 回归测试继续通过
5. 真实样本 benchmark 的热缓存构建耗时继续下降

## 11. 本轮实施结果

本轮已经完成实现并验证：

- [data/cache_store.py](C:/Users/ai/Desktop/可转债多因子/data/cache_store.py)
  - 新增 `time_series_aggregate` 读写能力
- [data/cache/service.py](C:/Users/ai/Desktop/可转债多因子/data/cache/service.py)
  - 新增聚合缓存 frame / metadata 读写与覆盖判断
  - 新增聚合缓存 stats 统计
- [data/data_loader.py](C:/Users/ai/Desktop/可转债多因子/data/data_loader.py)
  - `get_cb_daily_cross_section(...)` 支持 `aggregate_profile`
  - 为 `cb_daily_cross_section` 增加按月聚合缓存命中与按需物化逻辑
- [scoring_exports.py](C:/Users/ai/Desktop/可转债多因子/scoring_exports.py)
  - 因子打分链路接入 `factor_history_v1`

测试结果：

- `python -m unittest tests.test_cache_service tests.test_data_loader tests.test_scoring_exports tests.test_factor tests.test_derived_metrics -v`
- `42` 个测试全部通过

真实样本 benchmark：

- 对比口径：两只转债、`2025-10-01 ~ 2026-04-17`、`--skip-write`
- 上一轮基线：纯构建约 `5.5s`
- 第一次运行：
  - 纯构建约 `8.3s`
  - 同时建立 `factor_history_v1` 月度聚合缓存
- 第二次运行：
  - 纯构建约 `4.2s`
  - `cb_daily_cross_section` 不再读 214 个按日文件
  - 改为命中 `11` 个月度聚合缓存分片

结论：

- 这轮优化已经把最热链路从“按日小文件读取”进一步推进到了“按月聚合分片读取”
- 首次运行承担建缓存成本，第二次及后续重叠窗口运行收益明显
- 下一阶段如果继续优化，最值得评估的是：
  - 是否把 `factor_history_v1` 扩展为更多稳定 profile
  - 是否增加单次运行内的内存缓存，继续减少聚合分片的重复解析
