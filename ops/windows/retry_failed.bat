@echo off
setlocal
cd /d %~dp0\..\..
docker compose run --rm collector --mode retry-failed --base-dir /data %COLLECTOR_EXTRA_ARGS%
endlocal
