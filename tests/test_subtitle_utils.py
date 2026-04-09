import unittest

from syuka_ops.subtitle_utils import SubtitleSegment, dedupe_overlapping_segments


class SubtitleUtilsTestCase(unittest.TestCase):
    def test_dedupe_overlapping_segments_compacts_auto_caption_ladder(self) -> None:
        segments = [
            SubtitleSegment(11, "요 세 번째 주제 영국은 귀족들이"),
            SubtitleSegment(11, "요 세 번째 주제 영국은 귀족들이 지배하는 나라냐가 요번 주제가"),
            SubtitleSegment(12, "지배하는 나라냐가 요번 주제가"),
            SubtitleSegment(12, "지배하는 나라냐가 요번 주제가 되겠습니다. 아 이거 너무 재밌는"),
        ]

        compacted = dedupe_overlapping_segments(segments)

        self.assertEqual(
            compacted,
            [
                SubtitleSegment(
                    11,
                    "요 세 번째 주제 영국은 귀족들이 지배하는 나라냐가 요번 주제가 되겠습니다. 아 이거 너무 재밌는",
                )
            ],
        )

    def test_dedupe_overlapping_segments_keeps_unrelated_cues(self) -> None:
        segments = [
            SubtitleSegment(10, "첫 번째 문장입니다"),
            SubtitleSegment(14, "두 번째 문장입니다"),
        ]

        self.assertEqual(dedupe_overlapping_segments(segments), segments)

    def test_dedupe_overlapping_segments_tracks_last_cue_time(self) -> None:
        segments = [
            SubtitleSegment(9, "처음 시작해서 중요한 최신"),
            SubtitleSegment(37, "중요한 최신 정보를 담은 대국민 연설을 예고했습니다"),
            SubtitleSegment(41, "예고했습니다 대국민 연설하겠다"),
        ]

        self.assertEqual(
            dedupe_overlapping_segments(segments),
            [SubtitleSegment(9, "처음 시작해서 중요한 최신 정보를 담은 대국민 연설을 예고했습니다 대국민 연설하겠다")],
        )

    def test_dedupe_overlapping_segments_splits_long_auto_caption_runs(self) -> None:
        first = "가" * 500
        second = f"{'가' * 20}{'나' * 80}"

        compacted = dedupe_overlapping_segments(
            [
                SubtitleSegment(0, first),
                SubtitleSegment(10, second),
            ]
        )

        self.assertEqual(compacted, [SubtitleSegment(0, first), SubtitleSegment(10, "나" * 80)])


if __name__ == "__main__":
    unittest.main()
