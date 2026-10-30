@echo off
setlocal
cd /d "%~dp0"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\windows\Check-Domain.ps1"
set "ALORA_RESULT=%ERRORLEVEL%"
pause
exit /b %ALORA_RESULT%
