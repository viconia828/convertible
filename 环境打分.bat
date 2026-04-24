@echo off
setlocal
chcp 936 >nul
pushd "%~dp0"
echo ============================================================
echo 可转债多因子 - 环境打分导出
echo 说明:
echo 1. 请输入开始日期和结束日期（按区间导出）。
echo 2. 日期格式示例: 2026-04-01（格式: YYYY-MM-DD）
echo 3. 程序会输出一个逐日环境打分 XLSX 文件（适合查看环境切换和阶段变化）。
echo ============================================================
echo.
python "%~dp0tools\export_environment_scores.py" --interactive
echo.
pause
popd
endlocal
