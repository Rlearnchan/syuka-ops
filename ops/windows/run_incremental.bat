@echo off
setlocal
cd /d %~dp0\..\..
docker compose run --rm collector --mode incremental --base-dir /data %COLLECTOR_EXTRA_ARGS%
if errorlevel 1 goto :end
if defined SYUKA_ANALYSIS_API_KEY (
  docker compose run --rm collector --mode generate-analysis --base-dir /data --analysis-provider openai --analysis-limit 25 %COLLECTOR_ANALYSIS_EXTRA_ARGS%
) else (
  if defined OPENAI_API_KEY (
    docker compose run --rm collector --mode generate-analysis --base-dir /data --analysis-provider openai --analysis-limit 25 %COLLECTOR_ANALYSIS_EXTRA_ARGS%
  )
)
:end
endlocal
