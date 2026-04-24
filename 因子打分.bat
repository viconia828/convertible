@echo off
setlocal
chcp 936 >nul
pushd "%~dp0"
echo ============================================================
echo 可转债多因子 - 因子打分导出
echo 说明:
echo 1. 请输入开始日期和结束日期（按区间导出）。
echo 2. 请输入一个或多个可转债代码（支持 6 位代码或带交易所后缀的完整代码）。
echo 3. 代码可用逗号、中文逗号、空格分隔（示例: 110073.SH 128044.SZ）。
echo 4. 程序会校验单次运行代码上限，并输出逐日因子打分 XLSX（通常为每只债一个 sheet）。
echo ============================================================
echo.
python "%~dp0tools\export_factor_scores.py" --interactive
echo.
pause
popd
endlocal
