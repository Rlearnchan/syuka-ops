# syuka-ops

윈도우 서버에서 안정적으로 돌리는 것을 목표로 만든 운영용 프로젝트입니다.

우선 범위는 두 가지입니다.

1. 슈카월드 영상 메타데이터, 썸네일, 한국어 자막 수집
2. Slack에서 위 정보를 조회해서 직원 업무 지원

실험용 코드, 파인튜닝, ChromaDB는 우선 제외했고, 분석은 OpenAI 또는 Ollama 중 하나를 선택해 사용할 수 있습니다.

## 핵심 특징

- SQLite 기반 저장
- Docker 배포 우선
- Windows 작업 스케줄러 운영 가능
- 수동 실행 / 자동화 / 전체 / 증분 / 실패 재시도 / 일부 video_id 실행 지원
- 한국어 자막은 `수동 자막 우선`, 없으면 `자동 생성 한국어 자막`을 보조로 수집
- `Requested format is not available` 유형은 subtitle-only 경로에서 우회 처리
- 일부 `HTTP 429` 자동 자막은 브라우저 쿠키를 붙여 재시도 가능

## 폴더 구조

```text
syuka-ops/
├── src/syuka_ops/
├── data/
│   ├── db/
│   ├── scripts/raw/
│   └── thumbnails/
├── ops/windows/
├── docker-compose.yml
├── Dockerfile
├── Dockerfile.slack
└── pyproject.toml
```

## 데이터 저장 방식

SQLite 파일:

- `data/db/syuka_ops.db`

원본 파일:

- `data/scripts/raw/*.info.json`
- `data/scripts/raw/*.ko.vtt`

썸네일:

- `data/thumbnails/*.jpg`

주요 테이블:

- `videos`
- `transcripts`
- `download_attempts`

자막 관련 필드:

- `videos.has_ko_sub`: 수동 한국어 자막 존재
- `videos.has_auto_ko_sub`: 자동 생성 한국어 자막 존재
- `transcripts.subtitle_source`: 실제 수집된 전문의 출처 (`manual` 또는 `auto`)

예시 데이터:

```text
videos
- R0k8Gx9plVk | 2026-03-20 | 영국은 누가 소유하는가 | has_ko_sub=0 | has_auto_ko_sub=1
- GBUA74kDDiM | 2026-03-18 | 선거에서 지지 않는 묘수를 찾은 트럼프 | has_ko_sub=1 | has_auto_ko_sub=1

transcripts
- lj8Yc_Olp8U | manual | 834 segments
- Yikz6pf9XMA | auto | 1669 segments

raw files
- data/scripts/raw/2026-03-20__R0k8Gx9plVk__영국은 누가 소유하는가.info.json
- data/scripts/raw/2026-03-20__R0k8Gx9plVk__영국은 누가 소유하는가.ko.vtt
```

`info.json` 샘플 필드:

```text
id: R0k8Gx9plVk
title: 영국은 누가 소유하는가
upload_date: 20260320
view_count: 414783
like_count: 4556
subtitles_has_ko: False
automatic_captions_has_ko: True
```

## 기존 메타 이관

기존 `syuka-gpt`의 `meta.csv`만 먼저 가져와서 운영 DB를 채울 수 있습니다.
기존 `script.csv`는 신뢰도 이슈가 있으면 건너뛰고, 자막은 새 수집기로 다시 받는 흐름을 권장합니다.

```bash
PYTHONPATH=./src python3 -m syuka_ops.import_legacy_meta \
  --base-dir ./data \
  --meta-csv ../scripts/meta.csv \
  --legacy-raw-dir ../scripts/raw \
  --legacy-thumbnails-dir ../thumbnails \
  --copy-info-json \
  --copy-thumbnails
```

## 감사 명령

현재 DB와 로컬 자산 상태를 빠르게 확인할 수 있습니다.

```bash
PYTHONPATH=./src python3 -m syuka_ops.audit \
  --base-dir ./data \
  --command all \
  --expected-total 2215 \
  --limit 20 \
  --format markdown
```

지원 명령:

- `summary`
- `missing-info-json`
- `missing-transcripts`
- `subtitle-targets`
- `analysis-gaps`
- `integrity`
- `all`

감사에서 추가로 확인하는 항목:

- `videos` 대비 `transcripts` / `video_analysis` 누락
- `info_json_path` / `source_url` / `thumbnail_url` 누락
- 자막 플래그는 있는데 transcript가 없는 케이스
- transcript는 있는데 `subtitle_path`가 비었거나 실제 파일이 없는 케이스
- info.json skip 사유와 subtitle 실패 사유 분리

## 운영 진단

최근 운영 로그와 DB 상태를 한 번에 요약하려면:

```bash
PYTHONPATH=./src python3 -m syuka_ops.diagnostics \
  --base-dir ./data \
  --days 7 \
  --format markdown
```

생성 파일:

- `data/reports/syuka_ops_diagnostics_YYYYMMDD_HHMMSS.md`
- 또는 `data/reports/syuka_ops_diagnostics_YYYYMMDD_HHMMSS.json`

주요 지표:

- Slack request / DM / 버튼 액션 수
- 재연결 수 / BrokenPipe 수
- Collector 시작/완료 수
- 최근 N일 신규 영상 / 신규 transcript / 신규 analysis 수
- 현재 `info_json_missing` / `transcript_pending` / `analysis_missing` 수

## 수집 실행

로컬:

```bash
pip install -e .
syuka-collect --mode incremental --base-dir ./data
```

도커:

```bash
docker compose build
docker compose run --rm collector --mode incremental --base-dir /data
```

쿠키를 같이 써야 하는 환경이라면:

```bash
docker compose run --rm collector \
  --mode incremental \
  --base-dir /data \
  --cookies-from-browser chrome
```

OpenAI 분석 기본값:

- `SYUKA_ANALYSIS_PROVIDER=openai`
- `SYUKA_ANALYSIS_MODEL=gpt-5-mini`
- `SYUKA_ANALYSIS_BASE_URL=https://api.openai.com/v1`
- `OPENAI_API_KEY` 또는 `SYUKA_ANALYSIS_API_KEY`

최근 전문 중 아직 분석이 없는 건만 채우려면:

```bash
docker compose run --rm collector \
  --mode generate-analysis \
  --base-dir /data \
  --analysis-provider openai \
  --analysis-limit 25
```

메타/`info.json`만 먼저 보강하고 자막은 건너뛰려면:

```bash
docker compose run --rm collector \
  --mode backfill-info-json \
  --base-dir /data \
  --skip-transcripts \
  --skip-thumbnails \
  --video-batch-size 20 \
  --video-batch-index 1
```

조회수/좋아요 같은 변동 메타를 최근 영상 기준으로 다시 갱신하려면:

```bash
docker compose run --rm collector \
  --mode refresh-metrics \
  --base-dir /data \
  --skip-transcripts \
  --skip-thumbnails \
  --recent-days 90
```

전체 아카이브를 조금씩 순환 갱신하려면:

```bash
docker compose run --rm collector \
  --mode refresh-metrics \
  --base-dir /data \
  --skip-transcripts \
  --skip-thumbnails \
  --recent-days 0 \
  --video-batch-size 300 \
  --video-batch-index 1
```

이미 내려받은 `raw/*.info.json`만 DB에 다시 반영하려면:

```bash
docker compose run --rm collector \
  --mode refresh-local-info \
  --base-dir /data \
  --skip-transcripts \
  --skip-thumbnails
```

전체 수집을 자막 배치로 나눌 때:

```bash
docker compose run --rm collector \
  --mode full \
  --video-batch-size 50 \
  --video-batch-index 1 \
  --base-dir /data
```

실패 재시도:

```bash
docker compose run --rm collector --mode retry-failed --base-dir /data
```

실패 재시도에서도 쿠키를 같이 붙일 수 있습니다.

```bash
docker compose run --rm collector \
  --mode retry-failed \
  --base-dir /data \
  --cookies-from-browser chrome
```

특정 영상만 수동 수집:

```bash
docker compose run --rm collector \
  --mode retry-failed \
  --video-id ulsca8ME4ss \
  --video-id 2zuLQ-WZPXI \
  --base-dir /data
```

수집 현황을 엑셀 보고서로 정리하려면:

```bash
PYTHONPATH=./src python -m syuka_ops.report --base-dir ./data
```

생성 파일:

- `data/reports/syuka_collection_report_YYYYMMDD_HHMMSS.xlsx`
- 시트 구성: `Summary`, `Yearly`, `Videos`, `Issues`, `LatestAttempts`

## 운영 권장 흐름

초기 1회:

1. 메타 / `info.json` / 썸네일을 충분히 채웁니다.
2. 자막 배치를 여러 번 돌려 `transcripts`를 최대한 채웁니다.
3. 중간중간 `syuka_ops.report`로 엑셀 리포트를 생성해 공유합니다.

윈도우 서버 운영 단계:

- 하루 1~2회 `incremental` 실행이면 충분합니다.
- 이 모드는 최신 영상 쪽부터 새 메타와 새 자막 대상을 확인합니다.
- 이미 수집된 transcript는 자동 제외되므로, “추가된 것만” 갱신하는 운영에 적합합니다.
- `ops/windows/run_incremental.bat`는 수집 후 OpenAI 키가 있으면 최신 미분석 transcript에 대해 `generate-analysis`를 이어서 실행합니다.
- 조회수/좋아요는 별도로 `refresh-metrics`를 하루 1회 정도 돌리면 최신성이 좋아집니다.
- 추천은 `recent 90 days` 일일 갱신 + 전체 아카이브 소량 순환 갱신입니다.
- 리포트는 매 실행마다 만들기보다 3~5회 배치마다 한 번 생성하는 편이 효율적입니다.
- 과거 macOS `launchd` 경로 로그는 구 운영 흔적으로 보고, 현재 기준 운영은 Windows + Docker로 정리합니다.
- 최근 YouTube에서는 자막이 있는데도 `Requested format is not available`가 날 수 있습니다.
  이 프로젝트는 subtitle-only 경로에 `--ignore-no-formats-error`를 넣어 이 문제를 완화합니다.
- 일부 자동 자막은 `HTTP 429: Too Many Requests`가 날 수 있습니다.
  이 경우 브라우저 쿠키 또는 `cookies.txt`를 붙인 재시도가 효과적입니다.
- Windows 서버에서는 필요할 때 `COLLECTOR_EXTRA_ARGS` 환경변수로 추가 인자를 넣을 수 있습니다.

예시:

```bat
set COLLECTOR_EXTRA_ARGS=--cookies-from-browser chrome
ops\windows\run_incremental.bat
```

또는:

```bat
set COLLECTOR_EXTRA_ARGS=--cookies /data/youtube-cookies.txt
ops\windows\retry_failed.bat
```

권장 사항:

- YouTube 추출 안정성을 위해 `yt-dlp`는 주기적으로 업데이트합니다.
- 가능하면 JS runtime도 같이 준비합니다.
  참고: [yt-dlp EJS](https://github.com/yt-dlp/yt-dlp/wiki/EJS)

## 2026-04-09 운영 정리

현재 운영 기준은 Windows + Docker입니다. 과거 macOS `launchd` 경로는 구 운영 흔적으로 보고, 상시 실행은 `slack-bot`과 `collector-scheduler` 컨테이너를 기준으로 합니다.

주요 정리 내용:

- 패치노트 기준 오픈 흐름은 `2026-03-25` Slack 베타 오픈, `2026-04-01` 상시 구동 환경 정비, `2026-04-08` 수집 자동화/DB 정합성 점검 보강입니다.
- Slack Home은 DM 탭 사용을 전제로 단순화했습니다. 버튼은 `슈카월드`, `주제찾기`, `언급찾기`, `광고찾기`를 유지합니다.
- `전문` 보기는 Slack 한도에 맞춰 페이지네이션합니다. `다음 전문` / `이전 전문` 버튼으로 Slack 안에서 이어서 볼 수 있습니다.
- 자동 자막 전문은 Slack 표시 시 계단식 중복 cue를 정리합니다. 원본 VTT는 유지하고, 표시/파싱 단계에서만 `A`, `A+B`, `B`, `B+C` 패턴을 병합합니다.
- 수동 한국어 자막이 나중에 올라온 영상은 자동 자막 전문에서 수동 자막 전문으로 승격할 수 있게 했습니다. `kind=asr`는 자동 자막으로, `한국어` 트랙은 수동/제공 자막으로 판정합니다.
- DB 정합성 감사는 `info.json`, 썸네일, source URL, transcript 파일 존재, 자막 플래그와 실제 transcript 출처 불일치, 분석 누락을 함께 봅니다.
- OpenAI 분석은 daily 최신분은 즉시 처리하고, 과거 누락/품질 보정은 Batch API를 쓰는 구조입니다.
- `collector-scheduler`는 기본적으로 `09:00`, `18:00`, `00:00`에 incremental + 분석을 돌리고, `00:20`에 retry-failed를 돌립니다. 자정 시각은 내부적으로 `24:00` 대신 `00:00`으로 설정합니다.
- scheduler 로그는 `Asia/Seoul` 기준 시각으로 남기고, 예전 스케줄 키는 state 파일에서 정리합니다.
- 2026-04-09 점검에서 4월 8일 영상 1건 누락을 수동 incremental로 복구했고, 완료됐지만 적용되지 않았던 OpenAI backlog batch `131건`도 DB에 반영했습니다.

분석 비용 감각:

- 이미 분석된 영상은 다시 요청하지 않으므로 비용은 스케줄러 실행 횟수보다 신규 분석 영상 수에 비례합니다.
- `gpt-5.4-mini` 기준 Standard 단가는 입력 `$0.75 / 1M tokens`, 출력 `$4.50 / 1M tokens`입니다.
- Batch는 입력 `$0.375 / 1M tokens`, 출력 `$2.25 / 1M tokens`라 대량 백필에 적합합니다.
- 현재 파이프라인은 입력 전문을 `max_chars=15000`으로 자릅니다. 최신 영상 1편 분석은 대략 `$0.02~$0.04`, 한화로 약 30~60원 정도로 보는 것이 현실적입니다.
- 하루 1~2편 신규 분석만 생기는 평시 운영이라면 월 비용은 대략 `$1~$3` 수준을 예상합니다.

## Slack 봇

현재 상태:

- 직원용 Slack 조회 봇 베타 운영 가능
- App Home, 메시지 탭, `/syuka`, `@슈카창고` 모두 지원
- `video_analysis`가 DB 기준으로 채워져 있어 요약/키워드 활용 가능
- `핵심만 보기`는 가능하면 YouTube 챕터 + 자막 대목을 함께 보여줌
- `전문`은 타임스탬프가 붙은 자막 전체를 페이지 단위로 보여줌
- 자동 자막 전문은 표시 시 중복 cue를 줄여 읽기 좋게 정리함
- `광고찾기`는 명시적 광고 문구와 설명/제목 후보를 함께 보며 누락을 줄임
- 영상 상세 액션 버튼은 `전문`, `썸네일`, `유튜브`만 유지합니다.

환경 변수:

- `SLACK_BOT_TOKEN`
- `SLACK_APP_TOKEN`
- `SYUKA_DATA_DIR`
- `SLACK_ALLOWED_CHANNEL_IDS` (선택, 쉼표 구분)
- `SLACK_ALLOWED_USER_IDS` (선택, 쉼표 구분)
- `SYUKA_ANALYSIS_MODEL` (선택, 기본 `gpt-5-mini`)
- `SYUKA_ANALYSIS_BASE_URL` (선택, 기본 `https://api.openai.com/v1`)
- `SYUKA_ANALYSIS_PROVIDER` (선택, `ollama` 또는 `openai`)
- `SYUKA_ANALYSIS_API_KEY` (선택, OpenAI 사용 시 필요. 없으면 `OPENAI_API_KEY` 사용)

실행:

```bash
docker compose up slack-bot
```

지원 명령:

- `/syuka 슈카월드`
- `/syuka search 반도체`
- `/syuka search "AI 반도체" --limit 3 --page 2`
- `/syuka video ulsca8ME4ss`
- `/syuka 전문 ulsca8ME4ss`
- `/syuka transcript 관세`
- `/syuka transcript 관세 --limit 3 --page 2`
- `/syuka 광고찾기 한국거래소`
- `/syuka 썸네일 반도체`
- `/syuka collect-status`

앱 멘션으로도 같은 명령을 사용할 수 있습니다.
메시지 탭에서도 같은 키워드 기반 명령을 그대로 입력하면 됩니다.

Slack 응답 특징:

- `슈카월드`, `search`, `transcript`, `광고찾기`, `썸네일` 버튼 기반 탐색 지원
- 검색/최근/자막/광고 결과에 YouTube 바로가기 버튼 포함
- 썸네일 URL이 있으면 결과 카드에 미리보기 이미지 표시
- `search`, `transcript`, `thumbnail`, `ads`는 `--limit`, `--page` 옵션 지원
- `video`의 `핵심만 보기`는 챕터 기반 요약형
- `전문`은 페이지네이션되는 타임스탬프별 자막 전문형

### Slack 앱 설정

권장 방식은 Socket Mode입니다.

필수 설정:

- OAuth Scope
  - `app_mentions:read`
  - `channels:history`
  - `chat:write`
  - `commands`
  - `im:history`
  - `users:read`
- Slash Command
  - `/syuka`
- Event Subscriptions
  - `app_mention`
  - `app_home_opened`
  - `message.im`
- App Home
  - 홈 탭 활성화
  - 메시지 탭 활성화
- Socket Mode
  - 활성화
  - App Token 발급 (`connections:write`)

`.env` 예시:

```env
SLACK_BOT_TOKEN=xoxb-...
SLACK_APP_TOKEN=xapp-...
SYUKA_DATA_DIR=/data
SLACK_ALLOWED_CHANNEL_IDS=C01234567,C07654321
SLACK_ALLOWED_USER_IDS=U01234567
```

별도 Slack workspace용으로 토큰을 분리하고 싶다면 `.env.workspace`를 별도로 두는 방식을 권장합니다.

예:

```env
SLACK_BOT_TOKEN=xoxb-workspace-...
SLACK_APP_TOKEN=xapp-workspace-...
SYUKA_DATA_DIR=./data
SLACK_ALLOWED_CHANNEL_IDS=
SLACK_ALLOWED_USER_IDS=
```

Windows 서버에 1차로 Slack 봇만 올릴 때는 `.env.production.example`를 복사해 `.env`로 두는 방식을 권장합니다.
배포 순서는 `docs/windows-beta-deploy.md`를 참고하면 됩니다.

실행 예:

```bash
docker compose up -d slack-bot
```

맥과 Windows를 오가며 작업한다면 로컬 `.venv`를 공유하기보다 Docker 실행을 기본으로 두는 편이 안전합니다.
Mac에서 만든 `.venv`는 Windows에서 그대로 재사용되지 않을 수 있고, 반대도 마찬가지입니다.

권한 제한을 비워두면 설치된 워크스페이스에서 명령을 받습니다.
현재는 채널 제한 없이 App Home/메시지 탭에서 바로 쓰는 흐름도 지원합니다.

예전 `script.csv`의 요약/키워드를 DB로 옮기려면 Slack 실행과 별개로 아래 명령을 한 번 실행하면 됩니다.

```bash
PYTHONPATH=./src python -m syuka_ops.cli \
  --mode sync-legacy-analysis \
  --base-dir ./data \
  --script-csv ../scripts/script.csv
```

이후 Slack 봇은 CSV가 아니라 SQLite DB의 `video_analysis` 테이블을 읽습니다.

최신 transcript에 대해 요약/키워드를 생성해 DB에 바로 적재하려면 아래 명령을 사용합니다.

```bash
PYTHONPATH=./src python -m syuka_ops.cli \
  --mode generate-analysis \
  --base-dir ./data \
  --analysis-provider openai \
  --analysis-model gpt-5-mini \
  --analysis-base-url https://api.openai.com/v1 \
  --analysis-limit 10
```

참고:

- 기본 provider는 `openai`이고 기본 모델은 `gpt-5-mini`입니다.
- `--analysis-limit 0`이면 가능한 대상 전체를 순서대로 처리합니다.
- 기본은 `video_analysis`가 비어 있는 transcript만 처리합니다.
- 기존 요약/키워드까지 다시 만들려면 `--analysis-overwrite`를 붙이면 됩니다.

OpenAI API를 쓰고 싶다면:

```bash
export OPENAI_API_KEY=sk-...
PYTHONPATH=./src python -m syuka_ops.cli \
  --mode generate-analysis \
  --base-dir ./data \
  --analysis-provider openai \
  --analysis-model gpt-5-mini \
  --analysis-base-url https://api.openai.com/v1 \
  --analysis-limit 10
```

기본 추천 모델은 `gpt-5-mini`이고, 필요하면 환경에 맞춰 Ollama 모델이나 다른 OpenAI 모델로 바꿔 운영할 수 있습니다.
daily 최신분은 `generate-analysis`로 즉시 처리하고, 과거 누락/저품질 분석 보정은 Batch API를 쓰는 방식을 권장합니다.

실행 순서:

1. 먼저 `collector`로 데이터를 채웁니다.
2. Slack 앱을 워크스페이스에 설치합니다.
3. `.env`를 채웁니다.
4. `docker compose up -d slack-bot`으로 봇을 올립니다.
5. Slack에서 `/syuka 슈카월드`로 연결을 확인합니다.

## 수동 업데이트 체크리스트

현재 운영 감각상 업데이트는 아래처럼 나누는 것이 가장 효율적입니다.

1. 메타 갱신
- 목적: 최신 영상 목록, 조회수/좋아요, `info.json`, 설명, 챕터, 썸네일 URL 갱신
- 특징: 자막/분석보다 가볍고 자주 돌려도 부담이 적음
- 권장: 필요할 때 수동 실행, 또는 하루 1회 이상

2. 자막 갱신
- 목적: 새로 올라온 영상의 수동/자동 자막을 `transcripts`에 추가
- 특징: 자막은 한 번 확보되면 바뀔 일이 거의 없으므로 `incremental` 운영이 적합
- 권장: 과거 backlog 전체보다 최근 구간만 `incremental`
- 권장 예시: 마지막 자동 자막 시점부터 오늘까지, 또는 최근 30일

3. 분석 갱신
- 목적: 새 transcript에 대해 `video_analysis`의 요약/키워드 생성
- 특징: 이미 분석된 영상은 유지하고, 비어 있는 것만 채우는 방식이 적합
- 권장: 자막 갱신 뒤 최근 신규분만 `generate-analysis`

요약:

- 메타는 가볍고 자주
- 자막은 `incremental`
- 분석은 `incremental`

추천 수동 순서:

1. 메타 갱신
2. 자막 `incremental`
3. 분석 `generate-analysis`
4. Slack에서 `/syuka 슈카월드`, `/syuka 주제찾기 반도체`, `/syuka 광고찾기 한국거래소` 확인

최근분만 갱신하는 예시:

```bash
syuka-collect --mode incremental --base-dir ./data --date-from 2026-03-01
syuka-collect --mode generate-analysis --base-dir ./data --date-from 2026-03-01
```

기본 원칙:

- 메타는 전체 기준으로 가볍게 자주 갱신
- 자막은 최근 구간만 `incremental`
- 분석도 최근 신규분만 `generate-analysis`
- 과거 누락 자막은 꼭 필요할 때만 `video_id`나 기간을 지정해 예외적으로 복구

daily 운영 추천:

- `collector-scheduler`를 상시 실행
- 기본 증분 수집 시간은 `09:00`, `18:00`, `00:00`
- 기본 실패 재시도 시간은 `00:20`
- 분석은 새 transcript가 있을 때만 `generate-analysis`
- 과거 누락/품질 보정은 Batch API로 별도 처리
- 전체 메타 갱신은 월 1회 정도 별도로 실행

## GitHub 연동 메모

현재 워크스페이스에는 `.git` 디렉터리가 없고, 이 셸에서는 `git` 실행기도 잡히지 않습니다. 따라서 지금 이 세션에서 바로 GitHub remote를 붙이거나 push까지 하지는 못합니다.

자세한 순서는 [docs/GITHUB_SETUP.md](docs/GITHUB_SETUP.md)에 정리해두었습니다.

준비 조건:

- 로컬에 Git 설치
- 이 디렉터리에서 `git init`
- GitHub에 새 저장소 생성
- `git remote add origin <repo-url>`
- 첫 커밋 후 `git push -u origin main`

즉, 연동 자체는 가능합니다. 다만 현재 환경에서는 Git 실행기가 없어 “즉시 연결” 단계까진 못 간 상태입니다.

지원 조회 흐름:

- 최근 영상 조회
- 제목/자막 키워드 검색
- 특정 영상 상세 조회
- 자막 스니펫 검색
- 수집 상태 및 최근 자막 다운로드 시도 확인

## Windows 운영

`ops/windows/`에 바로 실행할 수 있는 배치 파일을 넣어두었습니다.

- `run_incremental.bat`
- `run_full_batch.bat`
- `retry_failed.bat`
- `register_collector_tasks.ps1`
- `unregister_collector_tasks.ps1`
- Windows 서버를 `WSL2 + Docker`로 운영하는 설계안:
  `docs/WINDOWS_WSL2_DOCKER_PLAN.md`
- Mac과 Windows를 오가며 쓸 때의 권장 방식:
  `docs/MAC_WINDOWS_SWITCHING.md`

Docker 반영 규칙도 간단히 기억해두면 편합니다.

- `.env`만 바뀌면: `docker compose up -d --force-recreate`
- 코드나 Dockerfile이 바뀌면: `docker compose build` 후 `docker compose up -d --force-recreate`

## 다음 추천 작업

1. `generated_ollama` / `legacy_script` 기반 구형 분석을 품질 기준으로 나눠 Batch API로 순차 재생성
2. `광고찾기` 후보 결과를 실제 Slack 사용 사례 기준으로 튜닝
3. `collect-status`와 diagnostics에서 수동/자동 자막 승격 후보를 계속 노출
4. SQLite FTS 또는 보조 인덱스로 `주제찾기` / `언급찾기` 검색 속도와 정렬 품질 개선
