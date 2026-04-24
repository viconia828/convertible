@echo off
setlocal
chcp 936 >nul
pushd "%~dp0"
echo ============================================================
echo 可转债多因子 - strategy 单交易日观察导出
echo 说明:
echo 1. 请输入一个交易日（必须是开市日）。
echo 2. 日期格式示例: 2026-04-20（格式: YYYY-MM-DD）
echo 3. 可选输入一组候选代码 / 观察名单（可不填；填写后会额外导出聚焦观察 sheet）。
echo 4. 本入口会输出 XLSX 文件（适合离线查看、对照排查、留档）。
echo 5. 如只想在终端快速查看结果（不导出文件），请改用: 策略预览.bat
echo ============================================================
echo.
python "%~dp0tools\export_strategy_observation.py" --interactive
echo.
pause
popd
endlocal
