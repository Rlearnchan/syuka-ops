# README Multi-Channel Addendum

## 2026-04-25 Update

`syuka-ops` now supports both:

- `슈카월드`
- `머니코믹스`

The collector defaults to all configured channels unless a specific channel is selected.

## New Slack Commands

Channel browse:

- `/syuka 슈카월드`
- `/syuka 머니코믹스`

Syukaworld shortcuts:

- `/syuka 월드주제 <키워드>`
- `/syuka 월드언급 <키워드>`
- `/syuka 월드광고 <키워드>`
- `/syuka 월드썸넬 <키워드 또는 video_id>`

MoneyComics shortcuts:

- `/syuka 머코주제 <키워드>`
- `/syuka 머코언급 <키워드>`
- `/syuka 머코광고 <키워드>`
- `/syuka 머코썸넬 <키워드 또는 video_id>`

Legacy generic commands such as `주제찾기`, `언급찾기`, `광고찾기`, `썸네일` remain available.

## Collector Notes

Default incremental:

```powershell
syuka-collect --mode incremental --base-dir ./data
```

Single channel:

```powershell
syuka-collect --mode incremental --base-dir ./data --channel-key moneymoneycomics
syuka-collect --mode incremental --base-dir ./data --channel-key syukaworld
```

## Analysis Notes

- MoneyComics is auto-subtitle-heavy
- OpenAI batch analysis is now part of the recommended workflow
- Use batch mode for historical backfill, then incremental analysis for daily updates

## Slack UI Notes

- Slack Home now presents:
  - channel entry points
  - channel-specific quick actions
  - multi-channel onboarding examples
