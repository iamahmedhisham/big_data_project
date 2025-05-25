@echo off
REM -------------------------------------------------------------
REM run_spark.bat
REM
REM This script checks Docker status, brings up Docker Compose,
REM then asks for a local Python file path to run on Spark in Docker.
REM -------------------------------------------------------------

:: 1. Check if Docker is running
echo Checking Docker status...
docker info >NUL 2>&1
IF %ERRORLEVEL% NEQ 0 (
    echo.
    echo Docker does not appear to be running.
    echo Please start Docker Desktop and then re-run this script.
    pause
    exit /b 1
)

:: 2. Ensure docker-compose cluster is up
echo.
echo Starting docker-compose cluster (if not already running)...
docker-compose up -d

:: 3. Ask for local Python file
echo.
echo Please enter the full path of your Python file (e.g. C:\path\to\my_script.py):
set /p SCRIPT_PATH=

:: 4. Copy file into spark-master container
echo.
echo Copying "%SCRIPT_PATH%" to spark-master:/tmp/my_script.py ...
docker cp "%SCRIPT_PATH%" spark-master:/tmp/my_script.py

:: 5. Run spark-submit inside the spark-master container
echo.
echo Running spark-submit inside spark-master ...
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /tmp/my_script.py

echo.
echo Done!
pause