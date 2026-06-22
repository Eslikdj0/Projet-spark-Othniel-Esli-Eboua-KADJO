@echo off
setlocal

set PORT=%1
if "%PORT%"=="" set PORT=8000
set SCRIPT_DIR=%~dp0

where python >nul 2>nul
if %ERRORLEVEL%==0 (
  python "%SCRIPT_DIR%serve.py" --port %PORT%
  exit /b %ERRORLEVEL%
)

where py >nul 2>nul
if %ERRORLEVEL%==0 (
  py -3 "%SCRIPT_DIR%serve.py" --port %PORT%
  exit /b %ERRORLEVEL%
)

echo Python is required to run the slide server.
exit /b 1
