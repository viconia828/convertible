# Macro Local Reference

当前阶段的宏观参考数据以本地 CSV 文件接入，后续再视稳定性决定是否替换为直连数据源。

当前文件示例：
- `treasury_10y.csv`
- `credit_spread.csv`
- `credit_spread.meta.json`

`credit_spread.csv` 当前口径：
- 来源：中国债券信息网 `https://valuation.chinabond.com.cn/cbweb-mn/yc/queryYz`
- 定义：`10Y AA 企业债到期收益率 - 10Y 国债到期收益率`
- 曲线：
  - `2c90818812b319130112c279222836c3`：中债企业债收益率曲线(AA)
  - `2c9081e50a2f9606010a3068cae70001`：中债国债收益率曲线
- 频率：日频

当前刷新与兜底机制：
- 主源：`ChinabondQueryYzSource`
- 备用源接口：实现 `CreditSpreadReferenceSource` 的 `name` 和 `fetch(start_ts, end_ts)` 即可挂入
- 调用方式：`CreditSpreadReferenceUpdater(primary_source=..., backup_sources=[...])`
- 断源处理：若主源和已注册备用源都失败，则自动回退到本地最后一份 `credit_spread.csv`
- 状态文件：`credit_spread.meta.json` 记录当前模式、覆盖区间、来源表和生效源

字段要求：
- `trade_date`
- `value`

可选字段：
- `indicator_code`
- `source_table`
