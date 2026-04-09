from __future__ import annotations

import unittest

from syuka_ops.ad_utils import detect_paid_promotion


class AdUtilsTestCase(unittest.TestCase):
    def test_detect_paid_promotion_extracts_advertiser(self) -> None:
        result = detect_paid_promotion("한국거래소의 유료광고가 포함된 영상입니다.")
        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result["advertiser"], "한국거래소")

    def test_detect_paid_promotion_extracts_supported_message(self) -> None:
        result = detect_paid_promotion("본 영상은 농림수산식품교육문화정보원의 지원을 받아 제작되었습니다.")
        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result["advertiser"], "농림수산식품교육문화정보원")

    def test_detect_paid_promotion_fallback_keyword(self) -> None:
        result = detect_paid_promotion("이 콘텐츠는 협찬 안내 문구가 포함되어 있습니다.")
        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result["matched_text"], "설명에 광고/지원 문구 포함")
