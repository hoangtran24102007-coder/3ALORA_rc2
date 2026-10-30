@echo off
setlocal
cd /d "%~dp0"
where node >nul 2>nul
if errorlevel 1 (
  echo Can cai Node.js 24.x truoc khi chay.
  pause
  exit /b 1
)
node tools\v25-staging.cjs
if errorlevel 1 (
  echo.
  echo Ban thu nghiem chua khoi dong. Doc thong bao o tren.
)
pause
