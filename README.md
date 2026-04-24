# 可转债多因子慢策略

面向协作开发者的项目说明。

## 总目标

这个项目的目标不是先堆一个大而全的回测框架，而是先把可转债多因子策略最核心的三层打稳：

- 稳定的数据获取、缓存和补数链路
- 可解释的环境打分与个券因子打分
- 可继续衔接到权重映射、组合构建和后续回测的纯函数式策略骨架

最终希望形成一条清晰链路：

`原始数据 -> 环境分数 / 因子分数 -> 因子权重 -> 目标组合 -> 回测与验证`

## 当前进度

截至 `2026-04-22`，项目进度可以按阶段理解：

- 阶段 1，数据层、环境打分、因子打分：已基本可用
- 阶段 2，环境到因子权重的映射：已有 baseline 实现
- 阶段 3，策略引擎、组合构建、回测验证：已进入第一阶段骨架与单日期预览入口

当前已经落地的关键能力：

- 环境打分导出可用
  - 支持交互式输入和 CLI 参数
  - 支持 `run_summary`、中文注释、实际输出区间、数据质量状态
  - 数据不完整时会明确警告“不要直接用于投资判断”
- 因子打分导出可用
  - 单次支持多个代码导出
  - 输入六位代码时自动补交易所后缀
  - 输出改为“每个代码一个 sheet”
  - 单码导出时已修正为基于全市场截面计算，不再出现单标的自比较退化
- 数据层补数和缓存逻辑已收口
  - 修复了 `cb_equal_weight` 中间断档被误判为已覆盖的问题
  - 修复了本地阻断型代理导致 Tushare 无法连接的问题
  - 环境和因子链路都改成了“优先复用缓存，只补缺口”
- 性能已做过一轮实质优化
  - `cb_daily_cross_section`、`cb_daily`、`cb_rate` 都支持只补缺口和有限并发
  - 因子打分已改为全市场批量面板计算，而不是逐日全量重算
  - 因子预热历史已改为动态窗口，而不是固定长历史
  - 统一缓存层现在已能输出更统一的 hit / miss / partial hit、文件扫描、远端补数、回写和阶段耗时统计
- `strategy` 已具备单日期预览主链
  - 已可联通 `snapshot -> env/factors -> weights -> portfolio`
  - 支持单日期预览环境分数、因子权重和目标组合
  - 支持单交易日观察导出 XLSX，便于离线查看和留档
  - 支持为指定候选代码 / 观察名单补充聚焦视角，不改变全市场打分与选券口径
  - 默认走摘要模式，可通过 `--verbose` 展开完整诊断
  - 同一 `StrategyService` 实例内重复构建同一 `trade_date` 时，可短生命周期复用已装配 snapshot
  - 预览入口现在会直接显示缓存诊断摘要，便于判断 `runtime snapshot / request panel / aggregate` 三层收益
  - snapshot 历史缓冲现在已支持 `0 = 自动模式` 的共享 helper 语义；当前评估结论仍保留默认 `550`
  - 当前仍是“预览 / 联调入口”，还不是正式回测模块
- benchmark 入口现在也能直接输出更统一的缓存诊断
  - 保留原始 `cache_stats` / `cache_observability`
  - 新增累计 `cache_diagnostics`
  - 新增逐轮 `iteration_cache_diagnostics`，便于验证复跑收益
- 共享 helper 已继续收口环境导出 ready 时序语义
  - `warmup_first_ready_date` / `trend_first_ready_date` / `warmup_trade_days_excluded`
  - 环境导出主流程不再独占这些时间窗口解析逻辑

## 当前已完全可用的入口

如果是新协作者接手，现在可以直接把下面四个入口当成“已可日常使用”的主入口：

- 环境打分
  - 已可稳定导出指定日期区间的环境分数、ready 时序、summary 和数据质量提示
  - 适合先核对环境状态，再看后续策略权重映射
- 因子打分
  - 已可稳定导出指定日期区间、指定转债的全市场截面因子分数
  - 适合核对个券的五因子表现和排除原因
- 策略预览
  - 已可稳定预览单个交易日的环境分数、因子权重、目标组合和缓存诊断
  - 适合日常联调、看当日组合、带观察名单排查样本
- 策略观察导出
  - 已可稳定导出单个交易日的完整观察 XLSX
  - 适合留档、离线分析和逐券排查 `eligible / exclude_reason / total_score`

最常用的命令可以直接从这里开始：

```bash
python tools/export_environment_scores.py --interactive
python tools/export_factor_scores.py --interactive
python tools/preview_strategy.py --interactive
python tools/export_strategy_observation.py --interactive
```

如果更偏向桌面入口，也可以直接双击：

- [环境打分.bat](C:/Users/ai/Desktop/可转债多因子/环境打分.bat)
- [因子打分.bat](C:/Users/ai/Desktop/可转债多因子/因子打分.bat)
- [策略预览.bat](C:/Users/ai/Desktop/可转债多因子/策略预览.bat)
- [策略观察.bat](C:/Users/ai/Desktop/可转债多因子/策略观察.bat)

## 当前重要规则

这几条是当前项目已经确认的设计决策，后续开发请默认遵守：

- 缓存优先，但完整性优先级更高
  - 默认先用本地缓存
  - 如果覆盖不足，自动补远端数据
- 数据不完整时必须显式提示用户
  - 可以允许流程跑完
  - 但必须明确提示“当前计算结果可能有问题，不可直接用于投资判断”
- 不暴露“显式 cache-only 模式”给正式用户
  - 它只适合链路联调，不适合作为投资结果
- 刷新默认值放在参数文件中
  - 交互入口不再单独询问是否刷新
  - 单次运行仍可通过 CLI 参数覆盖
- 因子打分必须基于当日全市场可交易池做截面标准化
  - 不能在请求子集上做 zscore / percentile

## 主要入口

### 用户入口

- 环境打分：
  - [环境打分.bat](C:/Users/ai/Desktop/可转债多因子/环境打分.bat)
  - [export_environment_scores.py](C:/Users/ai/Desktop/可转债多因子/tools/export_environment_scores.py)
- 因子打分：
  - [因子打分.bat](C:/Users/ai/Desktop/可转债多因子/因子打分.bat)
  - [export_factor_scores.py](C:/Users/ai/Desktop/可转债多因子/tools/export_factor_scores.py)
- 策略预览：
  - [策略预览.bat](C:/Users/ai/Desktop/可转债多因子/策略预览.bat)
  - [preview_strategy.py](C:/Users/ai/Desktop/可转债多因子/tools/preview_strategy.py)
- 策略观察导出：
  - [策略观察.bat](C:/Users/ai/Desktop/可转债多因子/策略观察.bat)
  - [export_strategy_observation.py](C:/Users/ai/Desktop/可转债多因子/tools/export_strategy_observation.py)
- 参数文件：
  - [策略参数.txt](C:/Users/ai/Desktop/可转债多因子/策略参数.txt)
- 导出结果目录：
  - [导出结果](C:/Users/ai/Desktop/可转债多因子/导出结果)

### 代码主路径

- 数据层：
  - [data_loader.py](C:/Users/ai/Desktop/可转债多因子/data/data_loader.py)
  - [tushare_client.py](C:/Users/ai/Desktop/可转债多因子/data/tushare_client.py)
- 环境打分：
  - [environment_detector.py](C:/Users/ai/Desktop/可转债多因子/env/environment_detector.py)
  - [macro_alignment.py](C:/Users/ai/Desktop/可转债多因子/env/macro_alignment.py)
- 因子打分：
  - [factor_engine.py](C:/Users/ai/Desktop/可转债多因子/factor/factor_engine.py)
  - [exports/scoring_exports.py](C:/Users/ai/Desktop/可转债多因子/exports/scoring_exports.py)
- 权重映射：
  - [weight_mapper.py](C:/Users/ai/Desktop/可转债多因子/model/weight_mapper.py)
- 参数与配置解析：
  - [config/strategy_config.py](C:/Users/ai/Desktop/可转债多因子/config/strategy_config.py)

## 目录结构

```text
可转债多因子/
├─ data/        数据源、缓存、派生指标、交易日历
├─ env/         环境打分与宏观对齐
├─ factor/      个券因子打分
├─ model/       环境到因子权重的 baseline 映射
├─ tools/       交互式/CLI 导出入口
├─ tests/       单元测试与回归测试
├─ 导出结果/     用户导出文件
├─ 工作日志/     工作日志、开发路线、系统设计文档
└─ 策略参数.txt   用户可编辑参数
```

## 如何运行

推荐从项目根目录运行。

### 1. 环境打分

用途：

- 看指定日期区间的 `equity_strength / bond_strength / trend_strength`
- 同时检查 `warmup_first_ready_date / trend_first_ready_date / data_quality_status`

交互运行：

```bash
python tools/export_environment_scores.py --interactive
```

直接指定区间：

```bash
python tools/export_environment_scores.py --start-date 2026-03-01 --end-date 2026-04-22
```

### 2. 因子打分

用途：

- 看指定日期区间内某几只转债的五因子分数
- 同时检查 `eligible / exclude_reason / filter_diagnostics`

交互运行：

```bash
python tools/export_factor_scores.py --interactive
```

直接指定日期和代码：

```bash
python tools/export_factor_scores.py --start-date 2026-04-13 --end-date 2026-04-22 --codes "110073 128044"
```

### 3. 策略预览

用途：

- 看单个交易日的环境分数、因子权重和目标组合
- 日常默认建议先用这个入口

交互运行：

```bash
python tools/preview_strategy.py --interactive
```

直接指定交易日：

```bash
python tools/preview_strategy.py --trade-date 2026-04-20
```

策略预览详细模式：

```bash
python tools/preview_strategy.py --trade-date 2026-04-20 --verbose
```

策略预览观察名单模式：

```bash
python tools/preview_strategy.py --trade-date 2026-04-20 --codes "110073 128044"
```

### 4. 策略观察导出

用途：

- 导出单个交易日的完整观察 XLSX
- 更适合离线留档和排查样本，而不是终端快速浏览

交互运行：

```bash
python tools/export_strategy_observation.py --interactive
```

直接指定交易日和观察名单：

```bash
python tools/export_strategy_observation.py --trade-date 2026-04-20 --codes "110073 128044"
```

如果想固定输出路径：

```bash
python tools/export_strategy_observation.py --trade-date 2026-04-20 --output 导出结果\\my_observation.xlsx
```

也可以直接使用桌面批处理入口：

- [环境打分.bat](C:/Users/ai/Desktop/可转债多因子/环境打分.bat)
- [因子打分.bat](C:/Users/ai/Desktop/可转债多因子/因子打分.bat)
- [策略预览.bat](C:/Users/ai/Desktop/可转债多因子/策略预览.bat)
- [策略观察.bat](C:/Users/ai/Desktop/可转债多因子/策略观察.bat)

## 验证建议

当前回归测试主要集中在数据层、环境/因子导出和因子计算逻辑。协作开发前，建议先跑：

```bash
python -m unittest tests.test_strategy_config tests.test_tushare_client tests.test_data_loader tests.test_factor tests.test_scoring_exports -v
```

## 当前最值得继续做的事

如果是新的协作开发者接手，建议优先按这个顺序继续：

1. 在进入回测主链前，继续收口更稳定的缓存预热窗口约束与失效边界。
2. 为批量 preview / backtest 设计共享日期调度 helper，并显式区分 `fully-ready` 最小窗口与环境稳定评分窗口。
3. 在以上事项收口后，再继续往批量预览、调仓流程和回测主链路推进。

## 已知风险

- `credit_spread` 目前仍主要依赖中国债券信息网，备用源还未补齐。
- Tushare 仍然依赖本机网络与代理环境；如果出现大面积取数失败，先检查 `HTTP_PROXY / HTTPS_PROXY`。
- 因子打分已经完成主要性能优化，但全市场面板计算在大窗口下仍会占用较多内存。

## 相关文档

- [工作日志说明](C:/Users/ai/Desktop/可转债多因子/工作日志/README.md)
- [2026-04-24 工作日志](C:/Users/ai/Desktop/可转债多因子/工作日志/2026-04-24_工作日志.md)
- [2026-04-24 下一步清单](C:/Users/ai/Desktop/可转债多因子/工作日志/2026-04-24_下一步清单.md)
- [2026-04-24 factor_history_v1 扩展评估方案](C:/Users/ai/Desktop/可转债多因子/工作日志/2026-04-24_factor_history_v1扩展评估方案.md)
- [2026-04-24 factor_history_v1 扩展真实样本复核](C:/Users/ai/Desktop/可转债多因子/工作日志/2026-04-24_factor_history_v1扩展真实样本复核.md)
- [2026-04-24 DataCacheService 职责拆分方案](C:/Users/ai/Desktop/可转债多因子/工作日志/2026-04-24_DataCacheService职责拆分方案.md)
- [2026-04-24 缓存 schema / 口径版本治理方案](C:/Users/ai/Desktop/可转债多因子/工作日志/2026-04-24_缓存schema口径版本治理方案.md)
- [2026-04-24 缓存 schema / 口径版本治理实现记录](C:/Users/ai/Desktop/可转债多因子/工作日志/2026-04-24_缓存schema口径版本治理实现记录.md)
- [2026-04-24 strategy 运行期 snapshot 与底层缓存一致性方案](C:/Users/ai/Desktop/可转债多因子/工作日志/2026-04-24_strategy运行期snapshot与底层缓存一致性方案.md)
- [2026-04-24 strategy 运行期 snapshot 与底层缓存一致性实现记录](C:/Users/ai/Desktop/可转债多因子/工作日志/2026-04-24_strategy运行期snapshot与底层缓存一致性实现记录.md)
- [2026-04-24 环境历史缓冲动态收缩方案](C:/Users/ai/Desktop/可转债多因子/工作日志/2026-04-24_环境历史缓冲动态收缩方案.md)
- [2026-04-24 环境历史缓冲动态收缩实现记录](C:/Users/ai/Desktop/可转债多因子/工作日志/2026-04-24_环境历史缓冲动态收缩实现记录.md)
- [2026-04-24 strategy snapshot 历史缓冲动态收口方案](C:/Users/ai/Desktop/可转债多因子/工作日志/2026-04-24_strategy_snapshot历史缓冲动态收口方案.md)
- [2026-04-24 strategy snapshot 历史缓冲动态收口实现记录](C:/Users/ai/Desktop/可转债多因子/工作日志/2026-04-24_strategy_snapshot历史缓冲动态收口实现记录.md)
- [2026-04-24 strategy 默认历史缓冲切换评估](C:/Users/ai/Desktop/可转债多因子/工作日志/2026-04-24_strategy默认历史缓冲切换评估.md)
- [2026-04-22 工作日志](C:/Users/ai/Desktop/可转债多因子/工作日志/2026-04-22_工作日志.md)
- [2026-04-22 下一步清单](C:/Users/ai/Desktop/可转债多因子/工作日志/2026-04-22_下一步清单.md)
- [2026-04-22 strategy 预览观察名单视角方案](C:/Users/ai/Desktop/可转债多因子/工作日志/2026-04-22_strategy预览观察名单视角方案.md)
- [2026-04-22 strategy 预览 summary_verbose 开关方案](C:/Users/ai/Desktop/可转债多因子/工作日志/2026-04-22_strategy预览summary_verbose开关方案.md)
- [2026-04-22 环境 ready 时序共享 helper 方案](C:/Users/ai/Desktop/可转债多因子/工作日志/2026-04-22_环境ready时序共享helper方案.md)
- [2026-04-22 strategy snapshot 运行期复用方案](C:/Users/ai/Desktop/可转债多因子/工作日志/2026-04-22_strategy_snapshot运行期复用方案.md)
- [2026-04-22 统一缓存层观测指标方案](C:/Users/ai/Desktop/可转债多因子/工作日志/2026-04-22_统一缓存层观测指标方案.md)
- [2026-04-22 缓存诊断展示方案](C:/Users/ai/Desktop/可转债多因子/工作日志/2026-04-22_缓存诊断展示方案.md)
- [2026-04-22 strategy 单交易日观察导出方案](C:/Users/ai/Desktop/可转债多因子/工作日志/2026-04-22_strategy单交易日观察导出方案.md)
- [2026-04-20 工作日志](C:/Users/ai/Desktop/可转债多因子/工作日志/2026-04-20_工作日志.md)
- [2026-04-20 下一步清单](C:/Users/ai/Desktop/可转债多因子/工作日志/2026-04-20_下一步清单.md)
- [可转债多因子慢策略_系统设计文档.md](C:/Users/ai/Desktop/可转债多因子/工作日志/可转债多因子慢策略_系统设计文档.md)
- [可转债多因子慢策略_开发路线.md](C:/Users/ai/Desktop/可转债多因子/工作日志/可转债多因子慢策略_开发路线.md)
