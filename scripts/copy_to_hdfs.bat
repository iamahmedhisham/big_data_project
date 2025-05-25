@echo off
REM -------------------------------------------------------------------
REM copy_to_hdfs.bat
REM
REM 1) Checks if Docker is running (docker info)
REM 2) docker-compose up -d
REM 3) Prompts for a local file/folder path
REM 4) Extracts the final name (file or folder) from that path
REM 5) Copies that folder/file into the Namenode container with the same name
REM 6) Puts it directly under HDFS root (/), then lists that root
REM -------------------------------------------------------------------

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

:: 3. Prompt for the local path (file or folder)
echo.
echo Please enter the full path of your file or folder:
echo Example: C:\path\to\mimic_db  (folder)
echo          C:\path\to\my_data.csv (file)
set /p SRC_PATH=

:: 4. Extract the final name from that path
FOR %%i IN ("%SRC_PATH%") DO SET LOCAL_NAME=%%~nxi

echo.
echo "SRC_PATH"   = %SRC_PATH%
echo "LOCAL_NAME" = %LOCAL_NAME%

:: 5. Copy the folder/file into the namenode container
echo.
echo Copying "%SRC_PATH%" to namenode:/%LOCAL_NAME% ...
docker cp "%SRC_PATH%" namenode:/"%LOCAL_NAME%"

:: 6. Put it in HDFS root ("/") and then list that root
echo.
echo Putting folder/file into HDFS root: /%LOCAL_NAME% ...
docker exec namenode bash -c "hdfs dfs -put -f /%LOCAL_NAME% /%LOCAL_NAME% && hdfs dfs -ls /"

echo.
echo Done!
pause