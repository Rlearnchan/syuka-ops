# Slack Info Tab Copy

## Recommended Copy

### Short Description

슈카월드와 머니코믹스 영상을 채널별로 조회하는 내부 검색 도구

### Long Description

슈카창고는 슈카월드와 머니코믹스 영상 데이터를 채널별로 조회하는 내부 검색 도구입니다.

- `주제`는 제목과 요약을 기준으로 관련 영상을 찾습니다.
- `언급`은 자막 문장과 시점을 기준으로 실제 발언 대목을 찾습니다.
- `광고`는 설명란 기준으로 광고 사례를 찾습니다.

소개된 명령어는 DM과 채널에서 모두 그대로 사용할 수 있습니다.
영상 ID를 알면 `/syuka video <video_id>` 로 상세를 바로 열 수 있습니다.

### Example Commands

- `/syuka 슈카월드`
- `/syuka 머니코믹스`
- `/syuka 월드주제 AI`
- `/syuka 월드언급 "자, 오늘의 주제 AI 빅뱅입니다"`
- `/syuka 머코광고 시킹알파`
- `/syuka video NwNvW0lLVtc`

## Slack Admin Update Guide

Slack 앱 관리 화면에서 아래 항목들을 이 문구로 바꾸면 됩니다.

1. `Basic Information` > `Display Information`
2. `Short description` 에 `Short Description` 문구 입력
3. `Long description` 또는 정보 탭 설명 영역에 `Long Description` 문구 입력
4. 필요하면 예시 명령은 앱 소개/가이드 영역에 `Example Commands` 그대로 추가

## Notes

- 홈 탭은 바로 눌러보는 용도
- 정보 탭은 이 도구가 무엇을 찾는지 설명하는 용도
- 그래서 정보 탭은 기능 정의와 호출 방식 위주로 짧게 유지하는 구성이 가장 깔끔함
