# Multi-Channel Phase 1 Plan

## Goal

Extend `syuka-ops` from a single-channel Syukaworld workflow into a multi-channel workflow that also supports MoneyComics while keeping the existing Slack bot and collector architecture.

## Channels

- `syukaworld`
  - Display name: `슈카월드`
  - URL: `https://www.youtube.com/@syukaworld/videos`
- `moneymoneycomics`
  - Display name: `머니코믹스`
  - URL: `https://www.youtube.com/@moneymoneycomics/videos`

## Phase 1 Scope

1. Add channel metadata to stored videos and query helpers.
2. Reuse the current collector for both channels.
3. Keep daily incremental collection and OpenAI batch analysis flow.
4. Add Slack command families:
   - `월드주제`, `월드언급`, `월드광고`, `월드썸넬`
   - `머코주제`, `머코언급`, `머코광고`, `머코썸넬`
5. Preserve existing commands for backward compatibility where practical.

## Implementation Notes

### DB

- Add `channel_key` and `channel_name` columns to `videos`.
- Default existing rows to `syukaworld` / `슈카월드`.
- Update browse/search/transcript/stat queries to accept optional `channel_key`.

### Collector

- Introduce a shared channel registry in config.
- Allow collector metadata refresh to run against all configured channels by default.
- Keep transcript/thumbnail/analysis reuse by continuing to operate on stored `video_id` rows.
- Save channel metadata from yt-dlp responses and local `info.json`.

### Slack

- Add command aliases that combine channel + feature:
  - `월드주제` -> `search` within `syukaworld`
  - `월드언급` -> `transcript` within `syukaworld`
  - `월드광고` -> `ads` within `syukaworld`
  - `월드썸넬` -> `thumbnail` within `syukaworld`
  - `머코주제` -> `search` within `moneymoneycomics`
  - `머코언급` -> `transcript` within `moneymoneycomics`
  - `머코광고` -> `ads` within `moneymoneycomics`
  - `머코썸넬` -> `thumbnail` within `moneymoneycomics`
- Keep `슈카월드` browse command for the existing channel.
- Add a browse command for `머니코믹스`.

### Analysis

- Reuse current OpenAI batch pipeline.
- Continue with one-time historical backfill, then daily incremental analysis.
- Treat MoneyComics as auto-subtitle-heavy and prefer GPT-derived summary/keywords over raw transcript readability.

## Deferred to Phase 2

- Shorts-specific UI and filters
- Auto-subtitle quality scoring
- Channel-specific home/help customization beyond basic examples
- Richer entity extraction schema
