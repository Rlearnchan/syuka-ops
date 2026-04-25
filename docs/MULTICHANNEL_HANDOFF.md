# Multi-Channel Handoff

## Scope Completed

- Added multi-channel support for `syukaworld` and `moneymoneycomics`
- Added channel metadata to `videos`
- Updated collector to support channel-aware runs
- Added Slack command families:
  - `월드주제`, `월드언급`, `월드광고`, `월드썸넬`
  - `머코주제`, `머코언급`, `머코광고`, `머코썸넬`
- Loaded MoneyComics metadata, transcripts, and OpenAI batch analysis

## Current Data State

- `syukaworld`
  - videos: 2246
  - transcripts: 2224
  - analysis: 2224
- `moneymoneycomics`
  - videos: 507
  - transcripts: 499
  - analysis: 499

## Validation Completed

- `python -m compileall src/syuka_ops`
- `python -m unittest tests.test_slack_bot -v`

## Suggested Commit Summary

Title:

```text
Add MoneyComics multi-channel support and Slack channel shortcuts
```

Body:

```text
- add channel registry and channel metadata columns
- make collector and DB queries channel-aware
- add Slack commands for world/moneycomics scoped search flows
- update Slack Home/help/examples for multi-channel onboarding
- load MoneyComics metadata, transcripts, and OpenAI analysis batch
- add multi-channel rollout docs and runbook
```

## Next Ideas

- Add Shorts-aware browse and filtering
- Split `collect-status` into per-channel sections
- Add channel tabs or channel summary cards in Slack Home
- Improve auto-subtitle cleanup for short-form videos
