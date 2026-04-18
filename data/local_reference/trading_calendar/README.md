# Trading Calendar Fallback

如果 Tushare 临时不可用，可以在这里放交易所来源的本地日历文件。

文件命名建议：

- `SSE.csv`
- `SZSE.csv`

字段要求：

- `exchange`
- `calendar_date`
- `is_open`
- `previous_open_date`
