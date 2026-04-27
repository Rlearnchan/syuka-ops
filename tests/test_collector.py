from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from syuka_ops.collector import build_transcript_payload


class CollectorTranscriptPayloadTestCase(unittest.TestCase):
    def test_build_transcript_payload_dedupes_auto_vtt(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            os.environ["SYUKA_DATA_DIR"] = temp_dir
            subtitle_path = Path(temp_dir) / "auto_sample.ko.vtt"
            subtitle_path.write_text(
                "\n".join(
                    [
                        "WEBVTT",
                        "",
                        "00:00:00.000 --> 00:00:01.000",
                        "<00:00:00.000><c>안녕하세요</c>",
                        "",
                        "00:00:01.000 --> 00:00:02.000",
                        "안녕하세요 <00:00:01.100><c>여러분</c>",
                        "",
                        "00:00:02.000 --> 00:00:03.000",
                        "안녕하세요 여러분",
                        "",
                    ]
                ),
                encoding="utf-8",
            )

            dialogue, segment_count = build_transcript_payload(subtitle_path, "auto")

            self.assertEqual(dialogue, "안녕하세요 여러분")
            self.assertEqual(segment_count, 1)

    def test_build_transcript_payload_keeps_manual_srt_as_is(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            subtitle_path = Path(temp_dir) / "manual_sample.ko.srt"
            subtitle_path.write_text(
                "\n".join(
                    [
                        "1",
                        "00:00:00,000 --> 00:00:01,000",
                        "안녕하세요",
                        "",
                        "2",
                        "00:00:01,000 --> 00:00:02,000",
                        "여러분",
                        "",
                    ]
                ),
                encoding="utf-8",
            )

            dialogue, segment_count = build_transcript_payload(subtitle_path, "manual")

            self.assertEqual(dialogue, "안녕하세요 여러분")
            self.assertEqual(segment_count, 2)


if __name__ == "__main__":
    unittest.main()
