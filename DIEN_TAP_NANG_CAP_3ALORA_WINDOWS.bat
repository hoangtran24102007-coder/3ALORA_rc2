@echo off
cd /d "%~dp0"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\windows\3Alora.ps1" -Action Rehearse
set "ALORA_EXIT=%ERRORLEVEL%"
pause
exit /b %ALORA_EXIT%
