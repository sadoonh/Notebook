@echo off
setlocal

REM Get the directory where the script is located
set "SCRIPT_DIR=%~dp0"
cd /D "%SCRIPT_DIR%"

REM Check for Python
python --version >nul 2>&1
if errorlevel 1 (
    echo Python could not be found. Please install Python and add it to your PATH.
    goto :eof
)

REM Check for pip
python -m pip --version >nul 2>&1
if errorlevel 1 (
    echo pip could not be found. Please ensure pip is installed for your Python.
    goto :eof
)

set "VENV_NAME=venv"

REM Check if virtual environment exists
if not exist "%VENV_NAME%\Scripts\activate.bat" (
    echo Creating virtual environment...
    python -m venv "%VENV_NAME%"
    if errorlevel 1 (
        echo Failed to create virtual environment. Please check your Python installation.
        goto :eof
    )
)

REM Activate virtual environment
echo Activating virtual environment...
call "%VENV_NAME%\Scripts\activate.bat"

REM Install/update dependencies
echo Installing/updating dependencies from requirements.txt...
pip install -r requirements.txt
if errorlevel 1 (
    echo Failed to install dependencies. Please check requirements.txt and your internet connection.
    call "%VENV_NAME%\Scripts\deactivate.bat"
    goto :eof
)

REM Launch the application
echo Launching DataNotebook...
python notebook_app.py

REM Deactivate (optional, as command prompt will close or user can do it)
REM echo To deactivate the virtual environment manually, type: call venv\Scripts\deactivate.bat
REM call "%VENV_NAME%\Scripts\deactivate.bat"

endlocal