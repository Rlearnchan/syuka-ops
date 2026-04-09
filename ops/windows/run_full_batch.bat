@echo off
setlocal
set BATCH_SIZE=%1
set BATCH_INDEX=%2
if "%BATCH_SIZE%"=="" set BATCH_SIZE=50
if "%BATCH_INDEX%"=="" set BATCH_INDEX=1
cd /d %~dp0\..\..
docker compose run --rm collector --mode full --video-batch-size %BATCH_SIZE% --video-batch-index %BATCH_INDEX% --base-dir /data %COLLECTOR_EXTRA_ARGS%
endlocal
