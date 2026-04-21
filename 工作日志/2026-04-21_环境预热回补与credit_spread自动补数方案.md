## 背景

环境打分目前仍有一个关键缺口：

1. 导出层虽然已经能识别 warm-up 区间，但当 `credit_spread` 本地 reference 的覆盖起点晚于请求预热起点时，系统不会像其他时序数据那样自动补数。
2. 当前 warm-up 计数口径仍然偏松，实际按的是“请求日前需要 `N-1` 个交易日观察”，而用户希望的是“请求日前完整预热 `N` 个交易日”，即请求首日之前要完整有 `20` 个交易日预热。

这会导致：

- 请求 `2025-01-01 ~ 2026-04-20` 时，虽然业务预期是把预热自动推到 `2025-01-01` 之前的 `20` 个交易日，并从请求首日开始正式输出；
- 但系统实际会因为 `credit_spread` 只从 `2025-01-01` 开始，继续把请求窗口内前 `19` 个交易日裁掉。

## 目标

把环境导出的默认模式真正落成：

- 缓存优先
- 覆盖不足时自动补数
- 补数失败时才退回已有缓存并继续给出 warm-up / 数据质量提示

同时把 warm-up 计数修正成：

- `warmup_observation_count = 20`
- 请求首日前必须已有完整 `20` 个交易日预热
- 首个正式环境日应是“预热之后的下一个请求窗口交易日”

## 方案

### 1. 环境导出先解析“预热所需历史起点”

在导出层先根据交易日历和 `warmup_observation_count`，反推出：

- 请求首日前所需的第 `20` 个交易日
- 这个交易日作为环境导出的最小预热起点

这一步不再用 `N-1`，而是用完整 `N` 个交易日。

### 2. `credit_spread` reference 默认模式下自动补数

在数据层增加 `credit_spread` 覆盖保障：

- 如果本地 reference 不能覆盖请求的起止日期；
- 默认模式下自动调用 `refresh_credit_spread_reference(...)`；
- 取回的新数据与旧 snapshot 合并后再落盘，而不是覆盖旧 snapshot。

这样下一次环境导出时，本地 reference 的 coverage 会自然前移。

### 3. 环境导出在 warm-up 解析前先触发 coverage 保障

环境导出链路改为：

1. 先解析预热所需起点
2. 先确保 `credit_spread` 能覆盖这个预热起点
3. 再探测 `local_env_history_start`
4. 再正式取环境所需宏观数据并计算

这样 `history_start_requested` / `history_start_used` 也会更贴近真实可导出的环境窗口。

## 代码落点

- `data/credit_spread_reference.py`
  - `refresh(...)` 改成“新取回数据 + 旧 snapshot 合并后保存”

- `data/data_loader.py`
  - 新增 `ensure_credit_spread_reference_coverage(...)`
  - `get_macro_daily(...)` 在读取 `credit_spread` 时，默认遇到 coverage 不足会自动补数

- `scoring_exports.py`
  - 新增“按完整 `N` 个交易日预热”解析导出 warm-up 起点
  - 在环境导出里先触发 `credit_spread` coverage 保障
  - 修正 `warmup_first_ready_date` 的推导逻辑

## 测试

需要补三类测试：

1. `credit_spread` refresh 后会和旧 snapshot 合并，而不是覆盖
2. `get_macro_daily(...)` 在 `credit_spread` coverage 不足时会自动补数
3. 环境导出在“请求首日前补足完整 `20` 个交易日预热”后，会从请求首日开始导出

## 预期结果

以 `2025-01-01 ~ 2026-04-20` 为例：

- 系统会先把 warm-up 自动推到 `2025-01-01` 之前的 `20` 个交易日
- 如果 `credit_spread` 本地 reference 不够，就自动补数
- 成功后，首个正式环境日应回到 `2025-01-01` 或其下一个交易日
- 不应再把请求窗口内前 `19` 个交易日当成 warm-up 截掉
