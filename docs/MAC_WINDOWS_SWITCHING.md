# Mac / Windows 오가며 쓰는 운영 메모

이 프로젝트는 Mac과 Windows를 오가며 써도 되지만, 실행 방식은 `Docker`를 기준으로 맞추는 편이 가장 안전합니다.

핵심 원칙은 아래 3가지입니다.

1. 코드와 `data/`는 같이 옮겨도 된다.
2. `.venv`는 운영체제마다 다시 만든다.
3. 가능하면 Windows에서는 `WSL2 + Docker`, Mac에서는 Docker Desktop 또는 로컬 Docker를 쓴다.

## 왜 이렇게 쓰는가

`syuka-ops`는 이미 Docker 우선 구조입니다.

- `docker-compose.yml`이 `./data`를 `/data`로 마운트함
- `collector`, `slack-bot` 모두 컨테이너 기준으로 실행 가능
- 로컬 Python 경로나 가상환경 경로 차이를 줄일 수 있음

반대로 `.venv`는 운영체제 종속성이 큽니다.

- Mac에서 만든 `.venv`는 보통 `./.venv/bin/...` 구조
- Windows에서 만든 `.venv`는 보통 `.\.venv\Scripts\...` 구조
- Python 실행 파일 경로와 native wheel이 달라 그대로 재사용하기 어렵다

즉, Mac과 Windows를 오갈 때 공유해야 하는 것은 `코드 + data`이고, 공유하지 말아야 하는 것은 `.venv`입니다.

## 권장 작업 방식

### 공통

- 프로젝트 루트는 그대로 유지
- `data/` 폴더도 같이 이동
- 실행은 가능하면 `docker compose ...`

### Mac

- 프로젝트 위치 예: `~/code/syuka-ops`
- Docker 기준 실행
- 필요하면 로컬 `.venv`는 Mac에서만 별도로 생성

### Windows

- 가장 권장: `WSL2 Ubuntu` 안에 프로젝트 두기
- 예: `~/code/syuka-ops`
- Docker Desktop은 `WSL2 engine` 사용
- Windows Task Scheduler가 `wsl.exe` 또는 `docker compose`를 호출

권장하지 않는 방식:

- Mac에서 만든 `.venv`를 Windows에서 그대로 사용
- Windows의 `C:\Users\...\Documents` 아래에서 로컬 Python만으로 운영
- OneDrive 동기화 폴더 안에서 `data/`를 직접 운영

## 무엇을 옮겨야 하나

옮겨도 되는 것:

- 프로젝트 코드 전체
- `data/db/`
- `data/scripts/raw/`
- `data/thumbnails/`
- `data/reports/`
- `ops/windows/`
- `docs/`

각 OS에서 다시 만들 것:

- `.venv`
- OS별 셸 프로필 설정
- 필요 시 `.env`

민감정보라서 별도 관리가 더 좋은 것:

- `.env`
- `data/youtube-cookies.txt`

## 가장 안전한 실행 예시

수집기:

```bash
docker compose run --rm collector --mode incremental --base-dir /data
```

Slack 봇:

```bash
docker compose up -d slack-bot
```

실패 재시도:

```bash
docker compose run --rm collector --mode retry-failed --base-dir /data
```

쿠키 사용:

```bash
docker compose run --rm collector \
  --mode incremental \
  --base-dir /data \
  --cookies /data/youtube-cookies.txt
```

## Windows에서 특히 주의할 점

1. Docker Desktop이 떠 있어야 함
2. Docker가 현재 사용자 권한으로 실행 가능해야 함
3. `C:\Users\...\ .docker\config.json` 권한 문제가 있으면 Docker CLI가 경고를 낼 수 있음
4. 로컬 PowerShell 인코딩 때문에 한글이 깨져 보여도 파일 자체는 멀쩡할 수 있음
5. `.venv/bin/...` 경로가 보이면 그건 Mac에서 만든 환경일 가능성이 높음

## 빠른 점검 체크리스트

프로젝트를 다른 OS로 옮긴 직후에는 아래만 확인하면 됩니다.

1. `README.md`가 `UTF-8`로 정상 표시되는지
2. 한글 파일명이 정상 표시되는지
3. `data/db/syuka_ops.db`가 있는지
4. `data/scripts/raw/`와 `data/thumbnails/`가 있는지
5. `docker compose config`가 통과하는지
6. `docker compose run --rm collector --help`가 되는지

여기까지 되면 대개 운영 복구는 어렵지 않습니다.

## 결론

Mac과 Windows를 같이 쓴다면 이 프로젝트는 "로컬 Python 공유"가 아니라 "Docker 실행 공유"로 보는 편이 맞습니다.

정리하면:

- 공유: 코드, `data/`
- 재생성: `.venv`
- 권장 실행: Docker
- Windows 권장 환경: `WSL2 + Docker`
