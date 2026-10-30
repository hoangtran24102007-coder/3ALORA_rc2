@echo off
setlocal
title 3 ALORA - RESET ADMIN PASSWORD
cd /d "%~dp0"
echo.
echo ==========================================
echo      3 ALORA - RESET ADMIN PASSWORD
echo ==========================================
echo.
echo Cong cu nay DAT LAI mat khau tai khoan admin theo mat khau BAN TU CHON.
echo KHONG xoa don hang, QC, Packing, tai khoan nhan vien hay database.
echo Toan bo session Admin cu se bi thu hoi.
echo.
where node >nul 2>nul
if errorlevel 1 (
  echo [LOI] Chua tim thay Node.js 22 LTS.
  pause
  exit /b 1
)
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0RESET_ADMIN_PASSWORD.ps1"
echo.
pause
