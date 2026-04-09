# GitHub Setup

현재 워크스페이스는 아직 Git 저장소가 아닙니다.

확인된 상태:

- `.git` 디렉터리 없음
- 현재 셸에서 `git` 실행기 미탐지
- `.gitignore`는 운영 데이터가 올라가지 않도록 정리 완료

## 먼저 준비된 것

이미 아래 항목은 GitHub 업로드 기준으로 정리돼 있습니다.

- `.env`, `.env.company` 제외
- `data/db/*.db` 제외
- `data/scripts/raw/*` 제외
- `data/thumbnails/*` 제외
- `data/logs/*` 제외
- `data/reports/*` 제외
- `data/batches/*` 제외
- 빈 폴더 유지용 `.gitkeep` 추가

즉, 코드/문서/설정 템플릿 위주로 안전하게 올릴 수 있는 상태입니다.

## 권장 저장소 설정

- GitHub 저장소는 우선 `private`로 생성
- 기본 브랜치는 `main`
- 저장소 이름 예시: `syuka-ops`
- README는 로컬 파일을 쓰고 있으므로 GitHub 생성 시 기본 README는 만들지 않아도 됨

## 로컬에서 실행할 명령

Git이 설치된 뒤 이 디렉터리에서 아래 순서로 실행합니다.

```powershell
git init
git branch -M main
git add .
git commit -m "Initial commit"
git remote add origin <YOUR_GITHUB_REPO_URL>
git push -u origin main
```

예:

```powershell
git remote add origin https://github.com/<your-account>/syuka-ops.git
git push -u origin main
```

## 올리기 전에 확인할 것

- `.env`가 staging에 들어가지 않는지 확인
- `data/` 아래 실제 DB, 로그, 원본 자막, 썸네일이 staging에 들어가지 않는지 확인
- OpenAI 키, Slack 토큰, 회사 전용 URL이 문서에 하드코딩되지 않았는지 확인

추천 확인 명령:

```powershell
git status
git diff --cached
```

## 첫 공개 이후 권장 작업

1. GitHub 저장소 `Description` 추가
2. 기본 브랜치 보호 설정
3. 필요하면 `Issues` / `Projects` 활성화
4. 배포용 비밀값은 GitHub Secrets 또는 로컬 `.env` 유지

## 현재 세션에서 못 한 것

현재 Codex 세션에서는 `git` 실행기가 없어 아래 작업은 수행하지 못했습니다.

- `git init`
- commit 생성
- remote 연결
- GitHub push

즉, “GitHub에 올릴 준비”까지는 끝냈고, 실제 초기화/푸시는 Git이 설치된 뒤 바로 진행하면 됩니다.
