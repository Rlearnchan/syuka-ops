from __future__ import annotations

import csv
import sqlite3
import tempfile
import unittest
from pathlib import Path

from syuka_ops.db import connect, init_db, upsert_video
from syuka_ops.import_legacy_script import import_script_analysis


class ImportLegacyScriptTestCase(unittest.TestCase):
    def test_imports_summary_and_keywords_into_db(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base_dir = Path(temp_dir)
            script_csv = base_dir / "script.csv"
            with script_csv.open("w", encoding="utf-8-sig", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=["date", "title", "dialogue", "keyword", "summary", "video_id"])
                writer.writeheader()
                writer.writerow(
                    {
                        "date": "2026-03-20",
                        "title": "AI 반도체와 중국",
                        "dialogue": "반도체와 관세 이야기를 길게 설명합니다.",
                        "keyword": '["반도체", "관세", "중국"]',
                        "summary": "이 영상은 반도체와 관세 이슈를 설명합니다.",
                        "video_id": "abc123",
                    }
                )

            db_dir = base_dir / "db"
            db_dir.mkdir(parents=True, exist_ok=True)
            conn = connect(db_dir / "syuka_ops.db")
            init_db(conn)
            upsert_video(
                conn,
                {
                    "video_id": "abc123",
                    "title": "AI 반도체와 중국",
                    "upload_date": "2026-03-20",
                    "view_count": 123,
                    "like_count": 45,
                    "has_ko_sub": True,
                    "has_auto_ko_sub": False,
                    "thumbnail_url": None,
                    "source_url": "https://www.youtube.com/watch?v=abc123",
                    "info_json_path": None,
                },
            )
            conn.commit()
            conn.close()

            result = import_script_analysis(base_dir=base_dir, script_csv=script_csv)
            self.assertEqual(result["imported_analysis_rows"], 1)

            check_conn = sqlite3.connect(str(db_dir / "syuka_ops.db"))
            check_conn.row_factory = sqlite3.Row
            row = check_conn.execute("SELECT summary, keywords_json, analysis_source FROM video_analysis WHERE video_id = ?", ("abc123",)).fetchone()
            check_conn.close()

            self.assertIsNotNone(row)
            self.assertEqual(row["analysis_source"], "legacy_script")
            self.assertIn("반도체와 관세", row["summary"])
            self.assertIn("반도체", row["keywords_json"])


if __name__ == "__main__":
    unittest.main()
