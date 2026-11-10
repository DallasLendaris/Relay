@echo off
rem Double-click to run the targeted suite rerun (about 3 hours).
rem Needs Node 22+ (and Docker Desktop for t4). Git is not needed.
cd /d "%~dp0.."
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0run-targeted-reruns.ps1" %*
set RC=%ERRORLEVEL%
echo.
echo Finished (exit code %RC%). Results are in the results\rerun-* folder next to the repo.
pause
exit /b %RC%
