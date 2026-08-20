from __future__ import annotations

from common.keyword_watch import (
    match_keywords_in_text,
    normalize_keyword_term,
)


def test_trailing_asterisk_matches_normalized_root_variants():
    assert normalize_keyword_term(" Fotovolta* ") == "fotovolta*"
    assert match_keywords_in_text(
        "Panel fotovoltaico, solución fotovoltaica y módulos fotovoltaicos",
        ["fotovolta*"],
    ) == ["fotovolta*"]


def test_exact_words_and_phrases_keep_the_previous_behavior():
    text = "Sistema de agua helada con una UMA, no un equipo UMAC"
    assert match_keywords_in_text(
        text,
        ["agua helada", "uma", "fotovolta"],
    ) == ["agua helada", "uma"]

    assert match_keywords_in_text("panel fotovoltaico", ["fotovolta"]) == []
    assert match_keywords_in_text("equipo prefotovoltaico", ["fotovolta*"]) == []
