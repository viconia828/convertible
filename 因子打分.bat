@echo off
setlocal
chcp 65001 >nul
pushd "%~dp0"
echo ============================================================
echo 可转债多因子 - 因子打分导出
echo 说明:
echo 1. 请输入开始日期和结束日期。
echo 2. 请输入一个或多个可转债代码。
echo 3. 代码可用逗号、中文逗号、空格分隔。
echo 4. 程序内部会校验单次运行代码上限，并输出逐日因子打分 XLSX。
echo ============================================================
echo.
python "%~dp0tools\export_factor_scores.py" --interactive
echo.
pause
popd
endlocal
