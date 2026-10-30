$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
Write-Host ""
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "  3 ALORA - DAT LAI MAT KHAU ADMIN" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "Nhap mat khau tam thoi moi cho tai khoan admin." -ForegroundColor Yellow
Write-Host "Yeu cau: it nhat 10 ky tu, co chu HOA, chu thuong va so." -ForegroundColor Yellow
Write-Host "Mat khau KHONG hien tren man hinh va KHONG duoc ghi vao file." -ForegroundColor Green
Write-Host ""

function Read-PlainPassword([string]$Prompt) {
  $secure = Read-Host $Prompt -AsSecureString
  $ptr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure)
  try { return [Runtime.InteropServices.Marshal]::PtrToStringBSTR($ptr) }
  finally { [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($ptr) }
}

$p1 = Read-PlainPassword "Mat khau moi"
$p2 = Read-PlainPassword "Nhap lai mat khau"
if ($p1 -ne $p2) {
  Write-Host "[LOI] Hai mat khau khong trung nhau." -ForegroundColor Red
  exit 2
}
if ($p1.Length -lt 10 -or $p1 -notmatch '[A-Z]' -or $p1 -notmatch '[a-z]' -or $p1 -notmatch '[0-9]') {
  Write-Host "[LOI] Mat khau phai >=10 ky tu va co chu HOA, chu thuong, so." -ForegroundColor Red
  exit 2
}
try {
  $env:ALORA_ADMIN_RESET_PASSWORD = $p1
  & node "$PSScriptRoot\reset_admin_password.js"
  exit $LASTEXITCODE
}
finally {
  Remove-Item Env:ALORA_ADMIN_RESET_PASSWORD -ErrorAction SilentlyContinue
  $p1 = $null
  $p2 = $null
}
