@echo off
setlocal
chcp 65001 >nul
pushd "%~dp0"
echo ============================================================
echo 可转债多因子 - strategy 单日期预览
echo 说明:
echo 1. 请输入一个交易日。
echo 2. 日期格式示例: 2026-04-20
echo 3. 程序会输出当日环境分数、因子权重和目标组合。
echo ============================================================
echo.
python "%~dp0tools\preview_strategy.py" --interactive
echo.
pause
popd
endlocal
