@echo off
setlocal
cd /d "%~dp0"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\windows\3Alora.ps1" -Action RemoveService
set "ALORA_RESULT=%ERRORLEVEL%"
pause
exit /b %ALORA_RESULT%
