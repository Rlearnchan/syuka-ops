# Windows 1차 배포 체크리스트

목표:

- Windows 서버에 `syuka-ops`를 복사
- 기존 `data` 폴더를 그대로 옮김
- Docker로 `slack-bot`만 먼저 실행

이 단계에서는 수집기(`collector`)와 분석(`generate-analysis`)은 올리지 않습니다.

## 1. 준비물

Windows 서버에 필요한 것:

- Docker Desktop
- 이 저장소의 `syuka-ops/` 폴더
- 기존 운영 데이터 폴더 `syuka-ops/data/`
- 회사 Slack 앱에서 발급한 토큰 2개
  - `SLACK_BOT_TOKEN`
  - `SLACK_APP_TOKEN`

권장:

- 프로젝트 위치는 공백 없는 경로
  - 예: `C:\\syuka-gpt\\syuka-ops`

## 2. 복사할 것

Mac에서 아래 폴더를 통째로 옮깁니다.

- `syuka-ops/`

중요:

- `data/db/syuka_ops.db`
- `data/scripts/raw/`
- `data/thumbnails/`

위 3개가 같이 가야 Slack 봇이 바로 기존 검색 결과를 보여줄 수 있습니다.

## 3. 환경변수 파일 만들기

Windows 서버의 `syuka-ops/` 폴더 안에 `.env` 파일을 만듭니다.

예:

```env
SLACK_BOT_TOKEN=xoxb-...
SLACK_APP_TOKEN=xapp-...
SYUKA_DATA_DIR=/data
SLACK_ALLOWED_CHANNEL_IDS=
SLACK_ALLOWED_USER_IDS=
```

채널 제한 없이 쓰려면 빈 값으로 둡니다.

## 4. 첫 실행

PowerShell 또는 CMD에서:

```powershell
cd C:\syuka-gpt\syuka-ops
docker compose build slack-bot
docker compose up -d slack-bot
```

로그 확인:

```powershell
docker compose logs -f slack-bot
```

## 5. Slack에서 확인

아래 순서로 확인합니다.

1. App Home 열기
2. 메시지 탭에서 `슈카월드`
3. 메시지 탭에서 `주제찾기 반도체`
4. 메시지 탭에서 `언급찾기 관세`
5. 홈 버튼도 같이 눌러보기

## 6. 1차 배포에서 하지 않을 것

이번 단계에서는 아래는 생략해도 됩니다.

- Ollama 설치
- `generate-analysis`
- `collector incremental`
- daily update 자동화

즉, Slack 검색 봇만 먼저 검증합니다.

## 7. 문제 생기면 먼저 볼 것

### 봇이 안 뜰 때

- `.env`에 `xoxb`, `xapp`가 맞는지
- Slack 앱이 `Socket Mode`인지
- `docker compose logs -f slack-bot`

### 결과가 비어 있을 때

- `data/db/syuka_ops.db`가 같이 복사됐는지
- `data/scripts/raw/`가 같이 복사됐는지
- `SYUKA_DATA_DIR=/data`로 잡혔는지

### 썸네일/전문이 이상할 때

- `data/thumbnails/`
- `data/scripts/raw/`

폴더가 누락되지 않았는지 확인합니다.

## 8. 다음 단계

1차 배포가 안정적이면 그 다음에:

1. `collector` 컨테이너 추가
2. `cookies.txt` 기반 incremental 수집
3. Ollama + `generate-analysis`
4. daily update 자동화
