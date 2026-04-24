# 2026-04-24 `factor_history_v1` 扩展真实样本复核

## 1. 复核目的

基于 [2026-04-24_factor_history_v1扩展评估方案.md](C:/Users/ai/Desktop/可转债多因子/工作日志/2026-04-24_factor_history_v1扩展评估方案.md)，这轮真实样本复核要回答三件事：

1. `factor_history_v1` 当前到底吃掉了多少主路径收益。
2. `strategy` 单日期主链的复跑收益，主要来自 `factor_history_v1`，还是来自 `runtime snapshot reuse`。
3. `cb_equal_weight` 重建链是否已经强到值得单独再起第二个 aggregate profile。

## 2. 复核口径

### 2.1 真实样本代码

使用 `2026-04-23` 最新完整横截面里成交额最高的 20 只转债：

- `113618.SH`
- `113048.SH`
- `118033.SH`
- `113678.SH`
- `113597.SH`
- `118038.SH`
- `113051.SH`
- `123225.SZ`
- `113697.SH`
- `127096.SZ`
- `127024.SZ`
- `111000.SH`
- `111013.SH`
- `118004.SH`
- `123241.SZ`
- `127070.SZ`
- `123236.SZ`
- `118003.SH`
- `113039.SH`
- `123187.SZ`

### 2.2 复核窗口

因子基线窗口：

- `2025-10-01 ~ 2026-04-23`

重叠窗口：

- `2025-11-03 ~ 2026-04-23`

`strategy` 单日期：

- `2026-04-23`

`cb_equal_weight` 环境链观察窗口：

- `2024-10-01 ~ 2026-04-23`

## 3. 复核前顺手修掉的真实 blocker

在第一次跑大窗口 benchmark 时，发现 [append_weighted_total_score](C:/Users/ai/Desktop/可转债多因子/factor/factor_engine.py) 会在分数字段里混有 `pd.NA` 时直接报错。

真实触发路径是：

- 部分 `missing_daily_history` 行保留了 `pd.NA`
- 分数字段因此变成 `object`
- 总分汇总阶段直接 `.astype("float64")` 失败

本轮已修正为：

- 汇总前先对每个分数字段做 `pd.to_numeric(..., errors="coerce")`
- 让 `pd.NA` 统一按缺失值参与计算

并补了直接测试覆盖该场景。

## 4. `factor_history_v1` 冷热基线

### 4.1 cold baseline

命令：

```bash
python tools/benchmark_factor_export.py --start-date 2025-10-01 --end-date 2026-04-23 --codes "113618.SH,113048.SH,118033.SH,113678.SH,113597.SH,118038.SH,113051.SH,123225.SZ,113697.SH,127096.SZ,127024.SZ,111000.SH,111013.SH,118004.SH,123241.SZ,127070.SZ,123236.SZ,118003.SH,113039.SH,123187.SZ" --label factor_history_v1_cold_baseline --repeat 1 --skip-write
```

结果：

- `build_seconds = 6.154s`
- `request_panel_memory: misses=1 saves=1`
- `aggregate_memory: misses=11`
- `aggregate_metadata_memory: misses=11`
- 最大阶段耗时仍是 `cb_daily_cross_section::factor_history_v1::aggregate_lookup = 1388ms`

结论：

- 首轮长窗口构建里，`factor_history_v1` 的主要价值仍然成立
- 但首轮成本主要还集中在“逐月聚合分片读取与解析”

### 4.2 hot baseline

命令：

```bash
python tools/benchmark_factor_export.py --start-date 2025-10-01 --end-date 2026-04-23 --codes "113618.SH,113048.SH,118033.SH,113678.SH,113597.SH,118038.SH,113051.SH,123225.SZ,113697.SH,127096.SZ,127024.SZ,111000.SH,111013.SH,118004.SH,123241.SZ,127070.SZ,123236.SZ,118003.SH,113039.SH,123187.SZ" --label factor_history_v1_hot_baseline --repeat 3 --skip-write
```

结果：

- `iteration_build_seconds = [4.184, 3.024, 3.041]`
- 第 2、3 轮：
  - `request_panel_memory: hits=1`
  - `aggregate_memory: hits=0 misses=0`
  - `aggregate_metadata_memory: hits=0 misses=0`
- 第 2、3 轮 top stage 已不再是 `cb_daily_cross_section`，而转到：
  - `cb_basic`
  - `cb_rate`
  - `cb_call`

结论：

- 同窗口同进程复跑时，收益已经明显从“聚合分片”上移到了“request panel 命中”
- 从第 1 轮到第 2 轮，构建耗时从 `4.184s` 降到 `3.024s`
- 这说明对当前因子主链来说，`factor_history_v1 + request panel cache` 这一层已经比较完整

## 5. overlap 基线

在同一个 `DataLoader` 中连续跑：

1. `2025-10-01 ~ 2026-04-23`
2. `2025-11-03 ~ 2026-04-23`

结果：

- 第 1 个窗口：
  - `build_seconds = 3.373s`
  - `request_panel_memory: misses=1 saves=1`
  - `aggregate_memory: misses=11`
  - `aggregate_metadata_memory: misses=11`
- 第 2 个窗口：
  - `build_seconds = 3.018s`
  - `request_panel_memory: misses=1 saves=1`
  - `aggregate_memory: hits=10`
  - `aggregate_metadata_memory: hits=10`
  - `aggregate_lookup` 从 `1153ms` 降到 `867ms`

结论：

- 对“窗口大量重叠但不完全相同”的场景，当前收益主要来自：
  - 已解析的 aggregate frame / metadata memory hit
- 这条收益链已经存在
- 但它不是“第二个 profile”的证据，而是当前 `factor_history_v1` 本身的复用收益

## 6. `strategy` 单日期主链复核

在同一个 `StrategyService` 中连续跑：

1. `2026-04-23`，不带观察名单
2. `2026-04-23`，观察名单 A
3. `2026-04-23`，观察名单 B

结果：

- 第 1 次：
  - `build_seconds = 7.786s`
  - `runtime_snapshot_reused = false`
  - `request_panel_memory: misses=1 saves=1`
  - `aggregate_memory: misses=19`
  - `aggregate_metadata_memory: misses=19`
  - 最大阶段耗时仍是 `cb_daily_cross_section::factor_history_v1::aggregate_lookup = 2121ms`
- 第 2、3 次：
  - `build_seconds ≈ 1.0s`
  - `runtime_snapshot_reused = true`
  - `cache_diagnostics` 为“零增量”
  - 更换观察名单不再触发任何底层缓存活动

结论：

- `strategy` 单日期同日复跑的主要收益，不是来自“第二个横截面 profile”
- 而是来自：
  - `StrategyService` 的 runtime snapshot reuse
- 这说明当前 `strategy` 主链短期内没有再拆第二个 aggregate profile 的必要

## 7. `cb_equal_weight` 重建链复核

在同一个 `DataLoader` 中，连续两次请求：

- `get_cb_equal_weight_index("2024-10-01", "2026-04-23")`

结果：

- 第 1 次：
  - `build_seconds = 0.032s`
  - `cache_hits = 1`
  - `file_scans = 2`
  - 没有出现任何 `cb_daily_cross_section` 的 cache diagnostics
- 第 2 次：
  - `build_seconds = 0.027s`
  - 结果和第 1 次基本一致

补充判断：

- top stage 里出现的 `cb_equal_weight::rebuild_from_cross_section = 2ms`
  只是该分支的统一计时包装
- 从实际 cache delta 看，正常口径下它并没有触发新的 `cb_daily_cross_section` 读取

结论：

- `cb_equal_weight` 当前更多是“命中自身 time-series cache”的路径
- 它不是当前 steady-state 下的热点
- 因此也不足以支持“现在就该新增第二个 aggregate profile”

## 8. 最终结论

这轮真实样本复核后的结论很明确：

1. `factor_history_v1` 仍然是当前横截面聚合缓存的核心 profile。
2. 当前同窗口热跑收益，已经主要被 `request panel cache` 吸收。
3. 当前大量重叠窗口的收益，也已经能由 `factor_history_v1` 现有 aggregate memory 复用拿到。
4. `strategy` 单日期主链的重复运行，主要收益来自 `runtime snapshot reuse`，不是第二个 profile。
5. `cb_equal_weight` 在正常路径下主要命中自身缓存，不是当前足够强的第二候选。

因此当前阶段的推荐结论是：

- 暂不新增第二个 `cb_daily_cross_section` aggregate profile

## 9. 对下一步的影响

既然“扩第二个 profile”这条分支目前没有被真实样本支持，下一步更值得做的事就变成了：

1. 继续梳理 `DataCacheService` 的职责边界。
2. 为缓存层增加 schema / 口径版本治理。
3. 在回测模块真正起量后，如果出现新的稳定横截面列投影，再单独复评 aggregate profile。
