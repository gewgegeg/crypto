@echo off
setlocal
cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
  echo [build] Virtual environment not found at .venv\Scripts\python.exe
  echo Please create venv and install dependencies first.
  exit /b 1
)

".\.venv\Scripts\python.exe" -m pip install --upgrade pip setuptools wheel pyinstaller

if exist build rmdir /s /q build
if exist dist rmdir /s /q dist
del /q CryptoArb.spec 2>nul

".\.venv\Scripts\python.exe" -m PyInstaller --noconfirm --clean ^
  --name CryptoArb ^
  --collect-all ccxt ^
  --hidden-import win10toast ^
  --hidden-import aiodns ^
  --hidden-import aiohttp ^
  --hidden-import certifi ^
  --hidden-import idna ^
  --hidden-import charset_normalizer ^
  --hidden-import requests ^
  -w -F -i NONE run_gui.py

if exist dist\CryptoArb.exe (
  echo [build] Done: dist\CryptoArb.exe
) else (
  echo [build] Build finished but executable not found. See build logs above.
)

endlocal

