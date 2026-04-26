# 2026-04-26 Backfill Note

## Summary

- MoneyComics regular video metadata backfill completed.
- MoneyComics shorts metadata backfill completed.
- MoneyComics subtitles were backfilled in chunked runs to avoid SQLite lock contention.
- Shorts are now included in topic, transcript, ad, and thumbnail flows.
- A new OpenAI analysis batch was prepared and submitted for the remaining World + MoneyComics backlog, including shorts.

## Current Counts

- MoneyComics regular videos: `687`
- MoneyComics shorts: `584`
- MoneyComics regular transcripts: `678`
- MoneyComics shorts transcripts: `568`
- Remaining MoneyComics transcript targets: `12`

## OpenAI Batch

- Prepared rows: `753`
- Input path: `/data/batches/analysis_batch_20260426_003650.jsonl`
- Input file id: `file-NfJAwc7x9wWL5aVX3za5Lw`
- Batch id: `batch_69ed5e3af27c81908b19a92f863dd6cf`
- Initial status: `validating`

## Notes

- The remaining `12` transcript targets were intentionally left as-is after repeated collection attempts.
- Those rows are likely missing usable captions or hitting per-video collection issues rather than broad pipeline failures.
