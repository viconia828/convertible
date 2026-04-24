@echo off
setlocal
chcp 936 >nul
pushd "%~dp0"
echo ============================================================
echo 可转债多因子 - strategy 单日期预览
echo 说明:
echo 1. 请输入一个交易日（必须是开市日）。
echo 2. 日期格式示例: 2026-04-20（格式: YYYY-MM-DD）
echo 3. 可选输入一组候选代码 / 观察名单（仅用于聚焦查看，不改变全市场打分口径）。
echo 4. 本入口默认输出摘要模式（适合快速查看当日环境、权重、目标组合）。
echo 5. 如需完整诊断（含更详细提示），请改用命令行:
echo    python tools\preview_strategy.py --interactive --verbose
echo ============================================================
echo.
python "%~dp0tools\preview_strategy.py" --interactive
echo.
pause
popd
endlocal
