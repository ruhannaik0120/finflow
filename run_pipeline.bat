@echo off
setlocal
"%~dp0.venv\Scripts\python.exe" "%~dp0run_pipeline.py" %*
if errorlevel 9009 python "%~dp0run_pipeline.py" %*
exit /b %errorlevel%
