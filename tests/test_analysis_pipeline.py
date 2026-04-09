from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from syuka_ops.analysis_pipeline import AnalysisConfig, generate_video_analysis, sync_generated_analysis
from syuka_ops.db import connect, get_video, init_db, pending_video_analysis_rows, upsert_transcript, upsert_video, upsert_video_analysis


def _response(payload):
    class Response:
        def raise_for_status(self) -> None:
            return None

        def json(self):
            return payload

    return Response()


class AnalysisPipelineTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.base_dir = Path(self.temp_dir.name)
        self.conn = connect(self.base_dir / "syuka_ops.db")
        init_db(self.conn)

        upsert_video(
            self.conn,
            {
                "video_id": "done1",
                "title": "이미 분석된 영상",
                "upload_date": "2026-03-20",
                "view_count": 100,
                "like_count": 10,
                "has_ko_sub": True,
                "has_auto_ko_sub": False,
                "thumbnail_url": None,
                "source_url": "https://www.youtube.com/watch?v=done1",
                "info_json_path": None,
            },
        )
        upsert_transcript(
            self.conn,
            {
                "video_id": "done1",
                "dialogue": "관세와 반도체 이야기를 합니다.",
                "subtitle_path": None,
                "subtitle_source": "manual",
                "segment_count": 3,
            },
        )
        upsert_video_analysis(
            self.conn,
            {
                "video_id": "done1",
                "summary": "이미 요약된 영상입니다.",
                "keywords_json": json.dumps(["관세"], ensure_ascii=False),
                "analysis_source": "legacy_script",
            },
        )

        upsert_video(
            self.conn,
            {
                "video_id": "todo1",
                "title": "새로 분석할 영상",
                "upload_date": "2026-03-21",
                "view_count": 300,
                "like_count": 30,
                "has_ko_sub": True,
                "has_auto_ko_sub": False,
                "thumbnail_url": None,
                "source_url": "https://www.youtube.com/watch?v=todo1",
                "info_json_path": None,
            },
        )
        upsert_transcript(
            self.conn,
            {
                "video_id": "todo1",
                "dialogue": "미 하원과 쿠팡, 관세 인상 이야기를 길게 합니다.",
                "subtitle_path": None,
                "subtitle_source": "manual",
                "segment_count": 4,
            },
        )
        self.conn.commit()

    def tearDown(self) -> None:
        self.conn.close()
        self.temp_dir.cleanup()

    def test_pending_video_analysis_rows_skips_existing_by_default(self) -> None:
        rows = pending_video_analysis_rows(self.conn)
        self.assertEqual([row["video_id"] for row in rows], ["todo1"])

    def test_generate_video_analysis_parses_keywords_and_summary(self) -> None:
        config = AnalysisConfig(provider="ollama", model="gemma3:4b")
        with patch("syuka_ops.analysis_pipeline.requests.post") as mock_post:
            mock_post.side_effect = [
                _response({"message": {"content": '{"keywords": ["관세", "쿠팡", "미 하원", "무역", "한국", "조사", "수입품", "트럼프"]}'}}),
                _response({"message": {"content": "다음은 제공된 스크립트의 요약입니다.\n\n이 영상은 관세와 쿠팡 이슈를 설명합니다."}}),
            ]
            result = generate_video_analysis(
                config,
                title="미 하원 쿠팡 조사",
                upload_date="2026-03-21",
                dialogue="미 하원과 쿠팡, 관세 인상 이야기를 길게 합니다.",
            )

        self.assertEqual(result["keywords"][:3], ["관세", "쿠팡", "미 하원"])
        self.assertEqual(result["summary"], "이 영상은 관세와 쿠팡 이슈를 설명합니다.")

    def test_sync_generated_analysis_upserts_missing_rows(self) -> None:
        config = AnalysisConfig(provider="ollama", model="gemma3:4b")
        with patch("syuka_ops.analysis_pipeline.requests.get") as mock_get, patch(
            "syuka_ops.analysis_pipeline.requests.post"
        ) as mock_post:
            mock_get.return_value = _response({"models": [{"name": "gemma3:4b"}]})
            mock_post.side_effect = [
                _response({"message": {"content": '{"keywords": ["관세", "쿠팡", "미 하원", "무역", "한국", "조사", "수입품", "트럼프"]}'}}),
                _response({"message": {"content": "이 영상은 관세와 쿠팡 이슈를 설명합니다."}}),
            ]

            result = sync_generated_analysis(self.conn, config=config)

        self.assertEqual(result["processed_rows"], 1)
        row = get_video(self.conn, "todo1")
        self.assertIsNotNone(row)
        self.assertEqual(row["analysis_source"], "generated_ollama")
        self.assertIn("관세", row["summary"])
        self.assertIn("쿠팡", row["keywords_json"])

    def test_generate_video_analysis_supports_openai_provider(self) -> None:
        config = AnalysisConfig(
            provider="openai",
            model="gpt-4.1-mini",
            base_url="https://api.openai.com/v1",
            api_key="test-key",
        )
        with patch("syuka_ops.analysis_pipeline.requests.post") as mock_post:
            mock_post.side_effect = [
                _response({"output_text": '{"keywords": ["관세", "쿠팡", "미 하원", "무역", "한국", "조사", "수입품", "트럼프"]}'}),
                _response({"output_text": "이 영상은 관세와 쿠팡 이슈를 설명합니다."}),
            ]
            result = generate_video_analysis(
                config,
                title="미 하원 쿠팡 조사",
                upload_date="2026-03-21",
                dialogue="미 하원과 쿠팡, 관세 인상 이야기를 길게 합니다.",
            )

        self.assertEqual(result["keywords"][:3], ["관세", "쿠팡", "미 하원"])
        self.assertEqual(result["summary"], "이 영상은 관세와 쿠팡 이슈를 설명합니다.")

    def test_sync_generated_analysis_uses_openai_source_label(self) -> None:
        config = AnalysisConfig(
            provider="openai",
            model="gpt-4.1-mini",
            base_url="https://api.openai.com/v1",
            api_key="test-key",
        )
        with patch("syuka_ops.analysis_pipeline.requests.post") as mock_post:
            mock_post.side_effect = [
                _response({"output_text": '{"keywords": ["관세", "쿠팡", "미 하원", "무역", "한국", "조사", "수입품", "트럼프"]}'}),
                _response({"output_text": "이 영상은 관세와 쿠팡 이슈를 설명합니다."}),
            ]
            result = sync_generated_analysis(self.conn, config=config)

        self.assertEqual(result["processed_rows"], 1)
        row = get_video(self.conn, "todo1")
        self.assertIsNotNone(row)
        self.assertEqual(row["analysis_source"], "generated_openai")


if __name__ == "__main__":
    unittest.main()
