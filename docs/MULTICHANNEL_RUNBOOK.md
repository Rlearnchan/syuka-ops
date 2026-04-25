# Multi-Channel Runbook

## Overview

Phase 1 now supports two channels in the same database:

- `syukaworld`
- `moneymoneycomics`

Default collector runs now refresh all configured channels unless `--channel-key` or `--channel-url` is provided.

## Recommended Rollout Order

1. Run a metadata-only sample for `moneymoneycomics`
2. Confirm DB rows and Slack browse/search behavior
3. Run transcript + thumbnail collection
4. Generate OpenAI batch analysis
5. Switch to normal daily incremental operation

## Sample Commands

### 1. Metadata-only sample for MoneyComics

Use a small batch first:

```powershell
syuka-collect --mode incremental --base-dir ./data --channel-key moneymoneycomics --skip-transcripts --skip-thumbnails
```

### 2. Full metadata backfill for MoneyComics

```powershell
syuka-collect --mode full --base-dir ./data --channel-key moneymoneycomics --skip-transcripts --skip-thumbnails
```

### 3. Collect transcripts and thumbnails after metadata is present

```powershell
syuka-collect --mode incremental --base-dir ./data --channel-key moneymoneycomics
```

If cookies are required:

```powershell
syuka-collect --mode incremental --base-dir ./data --channel-key moneymoneycomics --cookies-from-browser chrome
```

### 4. Generate OpenAI analysis directly

```powershell
syuka-collect --mode generate-analysis --base-dir ./data --analysis-provider openai --analysis-limit 50
```

### 5. Prepare and submit OpenAI batch analysis

```powershell
syuka-collect --mode prepare-analysis-batch --base-dir ./data --analysis-model gpt-5-mini --analysis-limit 200
syuka-collect --mode submit-analysis-batch --base-dir ./data --analysis-batch-path ./data/batches/analysis_batch_YYYYMMDD_HHMMSS.jsonl
syuka-collect --mode sync-analysis-batches --base-dir ./data
```

## Docker Examples

### Incremental refresh for all channels

```powershell
docker compose run --rm collector --mode incremental --base-dir /data
```

### MoneyComics-only backfill

```powershell
docker compose run --rm collector --mode full --base-dir /data --channel-key moneymoneycomics --skip-transcripts --skip-thumbnails
```

### OpenAI batch analysis

```powershell
docker compose run --rm collector --mode prepare-analysis-batch --base-dir /data --analysis-model gpt-5-mini --analysis-limit 200
```

## Slack Smoke Test

After sample data is loaded, check:

1. `/syuka 머니코믹스`
2. `/syuka 머코주제 금리`
3. `/syuka 머코언급 금리`
4. `/syuka 머코광고 삼성`
5. `/syuka 머코썸넬 반도체`
6. `/syuka 슈카월드`
7. `/syuka 월드주제 반도체`

## Notes

- Existing rows default to `syukaworld` if they predate channel metadata.
- Scheduler does not need a separate channel loop if it already runs the collector in default incremental mode.
- `collect-status` remains a combined view across all channels in phase 1.
- Shorts-specific filtering is intentionally deferred to phase 2.
