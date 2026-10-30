@echo off
chcp 65001 >nul
setlocal
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\windows\Production-Gate.ps1" %*
if errorlevel 1 (
 echo [NO-GO] Khong duoc chuyen domain sang V25.
 pause
 exit /b 1
)
echo [CHECKED] Van can nghiem thu domain Windows va dong bo thuc te.
pause
