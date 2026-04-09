from __future__ import annotations
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path

from syuka_ops import slack_bot
from syuka_ops.db import init_db, record_attempt, upsert_transcript, upsert_video, upsert_video_analysis
from syuka_ops.text_utils import (
    build_llm_context,
    chunk_for_llm,
    compact_around_query,
    keyword_context_snippets,
    preview_excerpt,
    representative_points,
    transcript_stats,
)
from syuka_ops.subtitle_utils import match_seconds_for_excerpt


class SlackBotTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.original_allowed_channels = os.environ.get("SLACK_ALLOWED_CHANNEL_IDS")
        self.original_allowed_users = os.environ.get("SLACK_ALLOWED_USER_IDS")
        os.environ["SLACK_ALLOWED_CHANNEL_IDS"] = ""
        os.environ["SLACK_ALLOWED_USER_IDS"] = ""
        slack_bot.runtime_config.cache_clear()
        self.addCleanup(self.restore_env)

        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.data_dir = Path(self.temp_dir.name)
        self.subtitle_path = self.data_dir / "abc123.ko.srt"
        self.info_path = self.data_dir / "abc123.info.json"
        self.subtitle_path.write_text(
            "1\n00:00:00,000 --> 00:00:04,000\n반도체와 관세 이야기를 길게 설명합니다.\n\n"
            "2\n00:03:12,000 --> 00:03:16,000\n중국 반도체 독립도 함께 다룹니다.\n\n"
            "3\n00:05:10,000 --> 00:05:14,000\n시장 반응도 이어서 정리합니다.\n",
            encoding="utf-8",
        )
        self.info_path.write_text(
            (
                '{"description":"한국거래소의 유료광고가 포함된 영상입니다.\\n반도체 시장 이야기를 함께 다룹니다.",'
                '"chapters":['
                '{"start_time":0.0,"title":"Opening Topic","end_time":180.0},'
                '{"start_time":192.0,"title":"China Semiconductor","end_time":300.0},'
                '{"start_time":310.0,"title":"Market Reaction","end_time":420.0}'
                ']}'
            ),
            encoding="utf-8",
        )
        db_dir = self.data_dir / "db"
        db_dir.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(db_dir / "syuka_ops.db"))
        self.conn.row_factory = sqlite3.Row
        init_db(self.conn)
        self.addCleanup(self.conn.close)

        upsert_video(
            self.conn,
            {
                "video_id": "abc123",
                "title": "AI 반도체와 중국",
                "upload_date": "2026-03-20",
                "view_count": 123456,
                "like_count": 7890,
                "has_ko_sub": True,
                "has_auto_ko_sub": False,
                "thumbnail_url": "https://example.com/thumb.jpg",
                "source_url": "https://www.youtube.com/watch?v=abc123",
                "info_json_path": str(self.info_path),
            },
        )
        upsert_transcript(
            self.conn,
            {
                "video_id": "abc123",
                "dialogue": "<00:00:09.559><c>반도체와 관세 이야기를 길게 설명합니다.</c> 중국 반도체 독립도 함께 다룹니다. 시장 반응도 이어서 정리합니다.",
                "subtitle_path": str(self.subtitle_path),
                "subtitle_source": "manual",
                "segment_count": 42,
            },
        )
        upsert_video_analysis(
            self.conn,
            {
                "video_id": "abc123",
                "summary": "## 스크립트 요약 (5~10줄)\n\n이 영상은 반도체와 관세 이슈를 중심으로 중국 시장의 변화를 설명합니다.\n공급망과 시장 반응도 함께 정리합니다.",
                "keywords_json": '["반도체", "관세", "중국", "시장"]',
                "analysis_source": "legacy_script",
            },
        )
        upsert_video(
            self.conn,
            {
                "video_id": "new999",
                "title": "오늘의 국제 뉴스 정리",
                "upload_date": "2026-03-21",
                "view_count": 222222,
                "like_count": 3333,
                "has_ko_sub": True,
                "has_auto_ko_sub": False,
                "thumbnail_url": "https://example.com/thumb2.jpg",
                "source_url": "https://www.youtube.com/watch?v=new999",
                "info_json_path": str(self.info_path),
            },
        )
        upsert_transcript(
            self.conn,
            {
                "video_id": "new999",
                "dialogue": "여러 국제 이슈를 짧게 정리합니다. 끝부분에 반도체라는 단어가 한 번만 지나갑니다.",
                "subtitle_path": str(self.subtitle_path),
                "subtitle_source": "manual",
                "segment_count": 3,
            },
        )
        upsert_video_analysis(
            self.conn,
            {
                "video_id": "new999",
                "summary": "국제 뉴스 전반을 짧게 훑습니다.",
                "keywords_json": '["국제", "뉴스"]',
                "analysis_source": "legacy_script",
            },
        )
        upsert_video(
            self.conn,
            {
                "video_id": "compact1",
                "title": "중국청년 투자 열풍",
                "upload_date": "2026-03-18",
                "view_count": 55555,
                "like_count": 444,
                "has_ko_sub": True,
                "has_auto_ko_sub": False,
                "thumbnail_url": "https://example.com/thumb3.jpg",
                "source_url": "https://www.youtube.com/watch?v=compact1",
                "info_json_path": str(self.info_path),
            },
        )
        upsert_transcript(
            self.conn,
            {
                "video_id": "compact1",
                "dialogue": "중국청년 투자자들의 분위기와 시장 기대를 설명합니다.",
                "subtitle_path": str(self.subtitle_path),
                "subtitle_source": "manual",
                "segment_count": 2,
            },
        )
        upsert_video_analysis(
            self.conn,
            {
                "video_id": "compact1",
                "summary": "중국청년 투자자 흐름을 짚습니다.",
                "keywords_json": '["중국청년", "투자"]',
                "analysis_source": "legacy_script",
            },
        )
        record_attempt(
            self.conn,
            {
                "video_id": "abc123",
                "stage": "subtitle",
                "status": "downloaded",
                "attempts": 1,
            },
        )
        self.conn.commit()

    def restore_env(self) -> None:
        if self.original_allowed_channels is None:
            os.environ.pop("SLACK_ALLOWED_CHANNEL_IDS", None)
        else:
            os.environ["SLACK_ALLOWED_CHANNEL_IDS"] = self.original_allowed_channels
        if self.original_allowed_users is None:
            os.environ.pop("SLACK_ALLOWED_USER_IDS", None)
        else:
            os.environ["SLACK_ALLOWED_USER_IDS"] = self.original_allowed_users
        slack_bot.runtime_config.cache_clear()

    def test_parse_argument_options_supports_limit_and_page(self) -> None:
        query, limit, page = slack_bot.parse_argument_options('AI 반도체 --limit 3 --page 2')
        self.assertEqual(query, "AI 반도체")
        self.assertEqual(limit, 3)
        self.assertEqual(page, 2)

    def test_parse_recent_options_supports_plain_numbers(self) -> None:
        limit, page = slack_bot.parse_recent_options("7 3")
        self.assertEqual(limit, 7)
        self.assertEqual(page, 3)

    def test_parse_world_options_supports_year_sort_limit_and_page(self) -> None:
        year, sort, limit, page = slack_bot.parse_world_options("2025년 좋아요 --limit 5 --page 2")
        self.assertEqual(year, "2025")
        self.assertEqual(sort, "likes")
        self.assertEqual(limit, 5)
        self.assertEqual(page, 2)

    def test_parse_argument_options_defaults_to_eight(self) -> None:
        query, limit, page = slack_bot.parse_argument_options("")
        self.assertEqual(query, "")
        self.assertEqual(limit, 8)
        self.assertEqual(page, 1)

    def test_prepare_query_text_normalizes_quotes_and_reorders_suffix_command(self) -> None:
        self.assertEqual(slack_bot.prepare_query_text('“광고찾기 우리은행”'), "광고찾기 우리은행")
        self.assertEqual(slack_bot.prepare_query_text('"우리은행 광고찾기"'), "광고찾기 우리은행")

    def test_route_natural_query_maps_common_phrases(self) -> None:
        self.assertEqual(slack_bot.route_natural_query("반도체 영상 찾아줘"), "주제찾기 반도체")
        self.assertEqual(slack_bot.route_natural_query("관세 어디서 말했지?"), "언급찾기 관세")
        self.assertEqual(slack_bot.route_natural_query("우리은행 광고했던 영상 보여줘"), "광고찾기 우리은행")
        self.assertEqual(slack_bot.route_natural_query("이 영상 썸네일 먼저 보고 싶어"), "썸네일")

    def test_strip_bot_mention(self) -> None:
        stripped = slack_bot.strip_bot_mention("<@U123> recent 5", "U123")
        self.assertEqual(stripped, "recent 5")

    def test_access_policy_defaults_to_allow(self) -> None:
        slack_bot.runtime_config.cache_clear()
        self.assertTrue(slack_bot.is_request_allowed(channel_id="C1", user_id="U1"))

    def test_handle_query_recent(self) -> None:
        response = slack_bot.handle_query("recent 1", data_dir=str(self.data_dir))
        self.assertIn("최근 영상", response.text)
        self.assertTrue(response.blocks)

    def test_handle_query_world_latest(self) -> None:
        response = slack_bot.handle_query("슈카월드 최신 1", data_dir=str(self.data_dir))
        self.assertIn("슈카월드 최신순", response.text)
        self.assertIn("new999", response.text)
        self.assertIn("결과 1/3건", response.blocks[1]["text"]["text"])

    def test_handle_query_world_year_and_likes(self) -> None:
        upsert_video(
            self.conn,
            {
                "video_id": "year2025",
                "title": "2025년 인기 영상",
                "upload_date": "2025-01-10",
                "view_count": 99999,
                "like_count": 999,
                "has_ko_sub": True,
                "has_auto_ko_sub": False,
                "thumbnail_url": None,
                "source_url": "https://www.youtube.com/watch?v=year2025",
                "info_json_path": None,
            },
        )
        self.conn.commit()
        response = slack_bot.handle_query("슈카월드 2025년 좋아요", data_dir=str(self.data_dir))
        self.assertIn("슈카월드 2025년 좋아요순", response.text)
        self.assertIn("year2025", response.text)

    def test_handle_query_search(self) -> None:
        response = slack_bot.handle_query("search 반도체", data_dir=str(self.data_dir))
        self.assertIn("검색 결과", response.blocks[0]["text"]["text"])
        self.assertIn("결과 2/2건", response.blocks[1]["text"]["text"])
        self.assertIn("abc123", response.text)
        self.assertLess(response.text.find("abc123"), response.text.find("new999"))
        self.assertTrue(any("한줄 요약" in block.get("text", {}).get("text", "") for block in response.blocks if block["type"] == "section"))
        action_block = next(block for block in response.blocks if block["type"] == "actions")
        self.assertEqual(action_block["type"], "actions")
        self.assertTrue(action_block["elements"][0]["action_id"].startswith("run_command_"))

    def test_search_and_transcript_ignore_spacing(self) -> None:
        search_response = slack_bot.handle_query("주제찾기 중국 청년", data_dir=str(self.data_dir))
        self.assertIn("compact1", search_response.text)
        transcript_response = slack_bot.handle_query("언급찾기 중국 청년", data_dir=str(self.data_dir))
        self.assertIn("compact1", transcript_response.text)

    def test_handle_query_video(self) -> None:
        response = slack_bot.handle_query("video abc123", data_dir=str(self.data_dir))
        self.assertIn("제목: AI 반도체와 중국", response.text)
        self.assertIn("핵심만 보기", response.text)
        self.assertNotIn("LLM 청크", response.text)
        self.assertIn("키워드: 반도체, 관세, 중국, 시장", response.text)
        self.assertIn("(00:00)", response.text)
        self.assertIn("Opening Topic", response.text)
        self.assertFalse(any(
            element.get("text", {}).get("text") == "바로가기"
            for block in response.blocks if block["type"] == "actions"
            for element in block["elements"]
        ))
        self.assertTrue(any(
            element.get("text", {}).get("text") == "유튜브"
            for block in response.blocks if block["type"] == "actions"
            for element in block["elements"]
        ))
        self.assertTrue(any(
            element.get("text", {}).get("text") == "전문"
            for block in response.blocks if block["type"] == "actions"
            for element in block["elements"]
        ))

    def test_handle_query_full_transcript(self) -> None:
        response = slack_bot.handle_query("전문 abc123", data_dir=str(self.data_dir))
        self.assertIn("전문 보기", response.text)
        self.assertIn("(00:00)", response.text)
        self.assertIn("(03:12)", response.text)
        self.assertIn("반도체와 관세 이야기를 길게 설명합니다.", response.text)
        self.assertIn("중국 반도체 독립도 함께 다룹니다.", response.text)

    def test_handle_query_video_without_analysis_shows_fallback(self) -> None:
        upsert_video(
            self.conn,
            {
                "video_id": "raw001",
                "title": "요약 없는 영상",
                "upload_date": "2026-03-22",
                "view_count": 1000,
                "like_count": 100,
                "has_ko_sub": True,
                "has_auto_ko_sub": False,
                "thumbnail_url": None,
                "source_url": "https://www.youtube.com/watch?v=raw001",
                "info_json_path": None,
            },
        )
        upsert_transcript(
            self.conn,
            {
                "video_id": "raw001",
                "dialogue": "요약은 아직 없지만 전문은 있습니다. 핵심 대목은 바로 볼 수 있습니다.",
                "subtitle_path": str(self.subtitle_path),
                "subtitle_source": "manual",
                "segment_count": 2,
            },
        )
        self.conn.commit()

        response = slack_bot.handle_query("video raw001", data_dir=str(self.data_dir))
        self.assertIn("요약 상태", response.text)
        self.assertIn("요약과 키워드는 아직 준비 중입니다.", response.text)

    def test_display_cleanup_removes_summary_prefixes_and_low_signal_keywords(self) -> None:
        cleaned_summary = slack_bot.clean_summary_text(
            "## 스크립트 요약 (5~10줄)\n\n요약하자면, 핵심 내용은 다음과 같습니다.\n- 담합 문제를 설명합니다.\n- 주요 내용은 다음과 같습니다.\n- 교복 가격 구조를 짚습니다."
        )
        self.assertNotIn("##", cleaned_summary)
        self.assertNotIn("요약하자면", cleaned_summary)
        self.assertNotIn("핵심 내용은", cleaned_summary)
        self.assertNotIn("주요 내용은", cleaned_summary)
        self.assertIn("담합 문제를 설명합니다.", cleaned_summary)

        cleaned_keywords = slack_bot.display_keywords(
            ["아이디어", "무엇", "교복 가격 담합", "생활형 교복", "가격", "얘기", "품목별 캡"]
        )
        self.assertEqual(cleaned_keywords, ["교복 가격 담합", "생활형 교복", "가격", "품목별 캡"])
        self.assertEqual(slack_bot.display_keywords(["아이디어", "무엇", "가격"]), [])
        concise = slack_bot.concise_summary_preview(
            "다음은 제공된 스크립트의 요약입니다.\n\n이 영상은 반도체 이슈를 다룹니다. 중국 공급망도 함께 설명합니다. 마지막에는 시장 반응을 정리합니다."
        )
        self.assertTrue(concise.startswith("이 영상은 반도체"))
        self.assertFalse(concise.endswith("…"))

    def test_handle_query_thumbnail(self) -> None:
        response = slack_bot.handle_query("thumbnail abc123", data_dir=str(self.data_dir))
        self.assertIn("썸네일 보기", response.text)
        self.assertEqual(response.blocks[0]["text"]["text"], "영상 썸네일")

    def test_handle_query_thumbnail_by_keyword(self) -> None:
        response = slack_bot.handle_query("썸네일 반도체", data_dir=str(self.data_dir))
        self.assertIn("썸네일 후보", response.text)
        self.assertEqual(response.blocks[0]["text"]["text"], "썸네일 후보: 반도체")

    def test_handle_query_transcript(self) -> None:
        response = slack_bot.handle_query("transcript 관세", data_dir=str(self.data_dir))
        self.assertIn("abc123", response.text)
        self.assertTrue(response.blocks)
        self.assertIn("결과 1/1건", response.blocks[1]["text"]["text"])
        self.assertIn("맥락:", response.text)
        self.assertTrue(any("키워드" in block.get("text", {}).get("text", "") for block in response.blocks if block["type"] == "section"))
        self.assertTrue(any("이 표현이 나온 대목" in block.get("text", {}).get("text", "") for block in response.blocks if block["type"] == "section"))
        self.assertIn("(00:00)", response.text)
        self.assertIn("> (00:00)", response.text)
        self.assertIn("`관세`", response.text)

    def test_handle_query_ads(self) -> None:
        response = slack_bot.handle_query("광고찾기 한국거래소", data_dir=str(self.data_dir))
        self.assertIn("광고 사례", response.blocks[0]["text"]["text"])
        self.assertIn("결과 3/3건", response.blocks[1]["text"]["text"])
        self.assertIn("광고주", response.text)
        self.assertIn("한국거래소", response.text)

    def test_handle_query_ads_accepts_quotes_and_reverse_order(self) -> None:
        quoted = slack_bot.handle_query('“광고찾기 한국거래소”', data_dir=str(self.data_dir))
        reversed_query = slack_bot.handle_query('"한국거래소 광고찾기"', data_dir=str(self.data_dir))
        self.assertIn("한국거래소", quoted.text)
        self.assertIn("한국거래소", reversed_query.text)

    def test_handle_query_ads_includes_company_mention_candidates(self) -> None:
        candidate_info_path = self.data_dir / "ad_candidate.info.json"
        candidate_info_path.write_text(
            '{"description":"카카오의 새로운 금융 서비스와 앱 사용 흐름을 소개합니다."}',
            encoding="utf-8",
        )
        upsert_video(
            self.conn,
            {
                "video_id": "ad_candidate",
                "title": "카카오 금융 서비스 살펴보기",
                "upload_date": "2026-03-22",
                "view_count": 111,
                "like_count": 22,
                "has_ko_sub": False,
                "has_auto_ko_sub": False,
                "thumbnail_url": None,
                "source_url": "https://www.youtube.com/watch?v=ad_candidate",
                "info_json_path": str(candidate_info_path),
            },
        )
        self.conn.commit()

        response = slack_bot.handle_query("광고찾기 카카오", data_dir=str(self.data_dir))

        self.assertIn("ad_candidate", response.text)
        self.assertIn("후보", response.text)

    def test_handle_query_accepts_simple_natural_language(self) -> None:
        topic = slack_bot.handle_query("반도체 영상 찾아줘", data_dir=str(self.data_dir))
        self.assertIn("abc123", topic.text)
        mention = slack_bot.handle_query("관세 어디서 말했지?", data_dir=str(self.data_dir))
        self.assertIn("abc123", mention.text)
        ads = slack_bot.handle_query("한국거래소 광고했던 영상 보여줘", data_dir=str(self.data_dir))
        self.assertIn("한국거래소", ads.text)

    def test_pagination_actions_support_previous_and_next(self) -> None:
        pager = slack_bot.pagination_actions(command="search", query="반도체", limit=8, page=2, row_count=8)
        self.assertIsNotNone(pager)
        assert pager is not None
        labels = [element["text"]["text"] for element in pager["elements"]]
        self.assertEqual(labels, ["이전", "다음"])

    def test_handle_query_korean_aliases(self) -> None:
        response = slack_bot.handle_query("주제찾기 반도체", data_dir=str(self.data_dir))
        self.assertIn("abc123", response.text)
        response = slack_bot.handle_query("언급찾기 관세", data_dir=str(self.data_dir))
        self.assertIn("abc123", response.text)
        response = slack_bot.handle_query("도움말", data_dir=str(self.data_dir))
        self.assertIn("슈카창고 사용 안내", response.text)
        response = slack_bot.handle_query("영상 abc123", data_dir=str(self.data_dir))
        self.assertIn("AI 반도체와 중국", response.text)

    def test_handle_query_collect_status(self) -> None:
        response = slack_bot.handle_query("collect-status", data_dir=str(self.data_dir))
        self.assertIn("수집 상태", response.text)
        self.assertIn("영상 수", response.text)

    def test_app_home_view_contains_onboarding(self) -> None:
        view = slack_bot.app_home_view(user_name="테스터")
        self.assertEqual(view["type"], "home")
        rendered = "\n".join(block.get("text", {}).get("text", "") for block in view["blocks"] if "text" in block)
        self.assertIn("테스터님", rendered)
        self.assertIn("패치노트", rendered)
        self.assertIn("상시 구동 환경", rendered)
        self.assertIn("슈카월드", rendered)
        self.assertIn("주제찾기 반도체", rendered)
        self.assertIn("광고찾기 카카오", rendered)
        self.assertIn("@슈카창고 help", rendered)
        self.assertIn("리서처", rendered)
        self.assertIn("광고찾기", rendered)

    def test_app_home_result_view_contains_result_blocks(self) -> None:
        response = slack_bot.handle_query("슈카월드 최신 5", data_dir=str(self.data_dir))
        view = slack_bot.app_home_result_view(response, command="슈카월드 최신 5")
        self.assertEqual(view["type"], "home")
        rendered = "\n".join(block.get("text", {}).get("text", "") for block in view["blocks"] if "text" in block)
        self.assertIn("슈카월드", rendered)

    def test_examples_command(self) -> None:
        response = slack_bot.handle_query("추천질문", data_dir=str(self.data_dir))
        self.assertIn("추천질문", response.text)
        self.assertTrue(any(block["type"] == "actions" for block in response.blocks))

    def test_help_contains_onboarding_examples(self) -> None:
        response = slack_bot.handle_query("help", data_dir=str(self.data_dir))
        self.assertIn("처음 쓰는 분께 추천", response.text)
        self.assertIn("@슈카창고 help", response.text)
        self.assertIn("썸네일 반도체", response.text)
        self.assertEqual(response.blocks[3]["type"], "actions")

    def test_button_command_helper(self) -> None:
        button = slack_bot.button_command("상세", "video abc123")
        self.assertEqual(button["action_id"], "run_command")
        self.assertEqual(button["value"], "video abc123")

    def test_video_response_uses_readable_preview(self) -> None:
        response = slack_bot.handle_query("video abc123", data_dir=str(self.data_dir))
        self.assertIn("- (00:00) Opening Topic", response.text)
        self.assertIn("반도체와 관세 이야기를 길게 설명합니다.", response.text)
        self.assertIn("- (03:12) China Semiconductor", response.text)
        self.assertIn("중국 반도체 독립도 함께 다룹니다.", response.text)
        self.assertNotIn("<00:00:09.559>", response.text)
        self.assertIn("요약:", response.text)

    def test_no_results_responses_are_helpful(self) -> None:
        response = slack_bot.handle_query("주제찾기 없는키워드", data_dir=str(self.data_dir))
        self.assertIn("다시 시도해 보세요", response.text)
        response = slack_bot.handle_query("언급찾기 없는키워드", data_dir=str(self.data_dir))
        self.assertIn("다시 시도해 보세요", response.text)
        response = slack_bot.handle_query("썸네일 없는키워드", data_dir=str(self.data_dir))
        self.assertIn("다시 시도해 보세요", response.text)

    def test_unknown_command_response_is_helpful(self) -> None:
        response = slack_bot.handle_query("모르겠는명령", data_dir=str(self.data_dir))
        self.assertIn("이해하지 못했습니다", response.text)
        self.assertIn("/syuka 추천질문", response.text)

    def test_text_utils_prepare_chunks_for_future_llm_use(self) -> None:
        text = "첫 문장입니다. 둘째 문장입니다. 셋째 문장입니다. 넷째 문장입니다."
        preview = preview_excerpt(text, max_chars=30, max_sentences=2)
        self.assertIn("- 첫 문장입니다.", preview)
        stats = transcript_stats(text)
        self.assertGreaterEqual(stats["sentence_count"], 2)
        self.assertGreaterEqual(len(chunk_for_llm(text, target_chars=20)), 2)
        context = build_llm_context("테스트 제목", text, max_chars=30)
        self.assertIn("제목: 테스트 제목", context)

    def test_text_utils_strip_caption_markup_and_extract_context(self) -> None:
        text = "<00:00:09.559><c>재미 없는 거부터 합시다.</c> 그건 뭐냐? 관세 이야기입니다."
        points = representative_points(text, limit=2)
        self.assertEqual(points[0], "재미 없는 거부터 합시다.")
        snippets = keyword_context_snippets(text, "관세", max_snippets=1)
        self.assertIn("관세 이야기입니다.", snippets[0])

    def test_compact_around_query_keeps_query_visible(self) -> None:
        text = (
            "이게 무슨 내용이냐 지난 1월 27일 날 트럼프 대통령은 갑자기 한국 "
            "관세를 다시 25%로 올리겠다고 위협을 했습니다 한국이 지난해 체결된 "
            "무역협정을 이행하지 않고 국회에 통과를 안 시켜서 한국산 수입품에 대한 관세를 "
            "25% 다시 인상하겠다고 했습니다"
        )
        snippet = compact_around_query(text, "관세", max_chars=120)
        self.assertIn("관세", snippet)

    def test_match_seconds_for_excerpt_uses_best_matching_segment(self) -> None:
        excerpt = "중국 반도체 독립도 함께 다룹니다. 시장 반응도 이어서 정리합니다."
        self.assertEqual(match_seconds_for_excerpt(str(self.subtitle_path), excerpt), 192)


if __name__ == "__main__":
    unittest.main()
