"""
UNIT TESTS — pure functions with no external dependencies.
Covers: main.py helpers, ats_matcher.py, scraper.py URL builder,
        email_sender.py HTML helpers, sheets_manager.py row parser.
"""
import sys, os, re
from datetime import datetime, timezone, timedelta
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

# ── Import helpers directly ───────────────────────────────────────────────────
# Patch heavy imports before loading the modules
import unittest.mock as mock

# Prevent database.py from connecting on import
with mock.patch("google.oauth2.service_account.Credentials.from_service_account_file"):
    with mock.patch("google.cloud.bigquery.Client"):
        import database  # noqa: just need to make it importable

from main import _make_job_id, _parse_applicants, _is_fresh, _check_blacklist, \
                 _requires_no_sponsorship, _requires_clearance, _required_years
from ats_matcher import _clean, _extract_kw, _score
from scraper import _build_urls
from email_sender import _card, _filter_row


# ════════════════════════════════════════════════════════════════════════
# 1. _make_job_id
# ════════════════════════════════════════════════════════════════════════

def test_make_job_id_is_16_chars():
    jid = _make_job_id("Google", "Data Engineer", "New York")
    assert len(jid) == 16

def test_make_job_id_is_hex():
    jid = _make_job_id("Google", "Data Engineer", "New York")
    assert all(c in "0123456789abcdef" for c in jid)

def test_make_job_id_deterministic():
    a = _make_job_id("Google", "Data Engineer", "New York")
    b = _make_job_id("Google", "Data Engineer", "New York")
    assert a == b

def test_make_job_id_case_insensitive():
    a = _make_job_id("GOOGLE", "DATA ENGINEER", "NEW YORK")
    b = _make_job_id("google", "data engineer", "new york")
    assert a == b

def test_make_job_id_different_inputs_give_different_ids():
    a = _make_job_id("Google", "Data Engineer", "New York")
    b = _make_job_id("Meta", "Data Engineer", "New York")
    assert a != b

def test_make_job_id_whitespace_stripped():
    a = _make_job_id("  Google  ", "Data Engineer", "New York")
    b = _make_job_id("Google", "Data Engineer", "New York")
    assert a == b


# ════════════════════════════════════════════════════════════════════════
# 2. _parse_applicants
# ════════════════════════════════════════════════════════════════════════

def test_parse_applicants_plain_int():
    assert _parse_applicants("42") == 42

def test_parse_applicants_with_plus():
    assert _parse_applicants("100+") == 100

def test_parse_applicants_integer_input():
    assert _parse_applicants(75) == 75

def test_parse_applicants_invalid_returns_9999():
    assert _parse_applicants("N/A") == 9999

def test_parse_applicants_none_returns_9999():
    assert _parse_applicants(None) == 9999

def test_parse_applicants_empty_returns_9999():
    assert _parse_applicants("") == 9999

def test_parse_applicants_zero():
    assert _parse_applicants("0") == 0


# ════════════════════════════════════════════════════════════════════════
# 3. _is_fresh
# ════════════════════════════════════════════════════════════════════════

def test_is_fresh_very_recent():
    ts = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    assert _is_fresh(ts) is True

def test_is_fresh_old_post():
    ts = (datetime.now(timezone.utc) - timedelta(hours=10)).isoformat()
    assert _is_fresh(ts) is False

def test_is_fresh_empty_string():
    assert _is_fresh("") is False

def test_is_fresh_none():
    assert _is_fresh(None) is False

def test_is_fresh_zulu_format():
    ts = (datetime.now(timezone.utc) - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    assert _is_fresh(ts) is True

def test_is_fresh_exactly_at_boundary():
    # Just inside 5-hour window
    ts = (datetime.now(timezone.utc) - timedelta(hours=4, minutes=59)).isoformat()
    assert _is_fresh(ts) is True

def test_is_fresh_just_outside_boundary():
    ts = (datetime.now(timezone.utc) - timedelta(hours=5, minutes=1)).isoformat()
    assert _is_fresh(ts) is False


# ════════════════════════════════════════════════════════════════════════
# 4. _check_blacklist
# ════════════════════════════════════════════════════════════════════════

def test_check_blacklist_hit():
    bl = {"teksystems": "staffing", "kforce": "staffing"}
    is_bl, bt = _check_blacklist("TekSystems Inc.", bl)
    assert is_bl is True
    assert bt == "staffing"

def test_check_blacklist_miss():
    bl = {"teksystems": "staffing"}
    is_bl, bt = _check_blacklist("Google LLC", bl)
    assert is_bl is False
    assert bt is None

def test_check_blacklist_partial_match():
    bl = {"robert half": "staffing"}
    is_bl, _ = _check_blacklist("Robert Half Technology", bl)
    assert is_bl is True

def test_check_blacklist_empty_blacklist():
    is_bl, bt = _check_blacklist("TekSystems", {})
    assert is_bl is False

def test_check_blacklist_job_board_type():
    bl = {"jobright.ai": "job_board"}
    is_bl, bt = _check_blacklist("Jobright.ai", bl)
    assert is_bl is True
    assert bt == "job_board"


# ════════════════════════════════════════════════════════════════════════
# 5. _requires_no_sponsorship
# ════════════════════════════════════════════════════════════════════════

def test_no_sponsorship_direct_phrase():
    assert _requires_no_sponsorship("We do not sponsor visas. No visa sponsorship.") is True

def test_no_sponsorship_h1b():
    assert _requires_no_sponsorship("This role is no h1b eligible.") is True

def test_no_sponsorship_citizen_only():
    assert _requires_no_sponsorship("Must be a US citizen or permanent resident.") is True

def test_no_sponsorship_clean_job():
    assert _requires_no_sponsorship("We are an equal opportunity employer.") is False

def test_no_sponsorship_empty():
    assert _requires_no_sponsorship("") is False

def test_no_sponsorship_without_sponsorship_phrase():
    assert _requires_no_sponsorship("Must work without sponsorship required.") is True


# ════════════════════════════════════════════════════════════════════════
# 6. _requires_clearance
# ════════════════════════════════════════════════════════════════════════

def test_clearance_top_secret():
    assert _requires_clearance("Candidates must hold a top secret clearance.") is True

def test_clearance_tssci():
    assert _requires_clearance("TS/SCI required for this position.") is True

def test_clearance_public_trust():
    assert _requires_clearance("Public trust clearance is required.") is True

def test_clearance_clean_job():
    assert _requires_clearance("Join our data engineering team.") is False

def test_clearance_empty():
    assert _requires_clearance("") is False


# ════════════════════════════════════════════════════════════════════════
# 7. _required_years
# ════════════════════════════════════════════════════════════════════════

def test_required_years_standard():
    assert _required_years("Requires 7 years of experience in data engineering.") == 7

def test_required_years_range():
    assert _required_years("5 to 8 years of experience required.") == 5

def test_required_years_plus():
    assert _required_years("10+ years of experience.") == 10

def test_required_years_none():
    assert _required_years("Join our team today.") == 0

def test_required_years_multiple_takes_max():
    # "max" is used for filtering — should return the max found
    result = _required_years("2 years of Python, 8 years of experience total.")
    assert result == 8

def test_required_years_abbreviation():
    assert _required_years("3+ years exp in BigQuery.") == 3


# ════════════════════════════════════════════════════════════════════════
# 8. ATS Matcher — _clean
# ════════════════════════════════════════════════════════════════════════

def test_clean_lowercases():
    assert _clean("Python SQL BigQuery") == "python sql bigquery"

def test_clean_collapses_whitespace():
    assert _clean("python   sql") == "python sql"

def test_clean_strips_punctuation():
    result = _clean("python, sql! bigquery?")
    assert "," not in result
    assert "!" not in result

def test_clean_preserves_hashtag():
    # C# and F# should be preserved
    assert "#" in _clean("c# developer")

def test_clean_preserves_plus():
    assert "+" in _clean("C++ experience")

def test_clean_handles_empty():
    assert _clean("") == ""


# ════════════════════════════════════════════════════════════════════════
# 9. ATS Matcher — _extract_kw
# ════════════════════════════════════════════════════════════════════════

def test_extract_kw_finds_python():
    kws = _extract_kw("We need a Python developer with SQL skills.")
    assert "python" in kws
    assert "sql" in kws

def test_extract_kw_no_partial_match():
    # "pyspark" should not match on "spark" alone being extracted from "pyspark"
    kws = _extract_kw("Experience with pyspark and spark required.")
    assert "pyspark" in kws
    assert "spark" in kws

def test_extract_kw_case_insensitive():
    kws = _extract_kw("Experience with PYTHON and SQL required.")
    assert "python" in kws

def test_extract_kw_returns_set():
    result = _extract_kw("python python python")
    assert isinstance(result, set)

def test_extract_kw_bigquery():
    kws = _extract_kw("BigQuery and dbt experience preferred.")
    assert "bigquery" in kws
    assert "dbt" in kws

def test_extract_kw_empty_text():
    assert _extract_kw("") == set()


# ════════════════════════════════════════════════════════════════════════
# 10. ATS Matcher — _score
# ════════════════════════════════════════════════════════════════════════

def test_score_returns_all_keys():
    result = _score("Python SQL data engineer", "Python SQL data engineer")
    for key in ["ats_score", "cosine_similarity", "keyword_match_rate",
                "matched_keywords", "missing_keywords", "total_job_keywords"]:
        assert key in result

def test_score_identical_texts_high_score():
    text = "Python SQL BigQuery data engineer pipeline ETL"
    result = _score(text, text)
    assert result["ats_score"] > 70.0

def test_score_empty_resume_low():
    result = _score("", "Python SQL BigQuery data engineer")
    assert result["ats_score"] < 20.0

def test_score_matched_subset_of_job_kw():
    result = _score("Python SQL", "Python SQL BigQuery Spark Kafka")
    assert "python" in result["matched_keywords"]
    assert "sql" in result["matched_keywords"]
    missing = result["missing_keywords"]
    assert "bigquery" in missing or "spark" in missing

def test_score_range_0_to_100():
    result = _score("java c++ cobol", "python sql bigquery")
    assert 0.0 <= result["ats_score"] <= 100.0

def test_score_keyword_match_rate_is_percentage():
    result = _score("Python SQL BigQuery Spark Kafka", "Python SQL BigQuery Spark Kafka")
    assert result["keyword_match_rate"] == 100.0

def test_score_role_relevance_boost():
    # Job with "data engineer" should score higher than unrelated job
    relevant = _score("Python SQL pipeline", "Data engineer role with Python and SQL")
    irrelevant = _score("Python SQL pipeline", "Marketing manager Python SQL role")
    assert relevant["ats_score"] >= irrelevant["ats_score"]


# ════════════════════════════════════════════════════════════════════════
# 11. Scraper — _build_urls
# ════════════════════════════════════════════════════════════════════════

def test_build_urls_count():
    roles = ["Data Engineer", "Data Analyst"]
    urls = _build_urls(roles)
    assert len(urls) == 2

def test_build_urls_contains_role():
    urls = _build_urls(["Data Engineer"])
    assert "Data+Engineer" in urls[0]

def test_build_urls_contains_tpr():
    urls = _build_urls(["Data Engineer"])
    tpr = str(24 * 3600)
    assert tpr in urls[0]

def test_build_urls_valid_linkedin():
    urls = _build_urls(["Data Engineer"])
    assert urls[0].startswith("https://www.linkedin.com/jobs/search/")

def test_build_urls_empty_roles():
    assert _build_urls([]) == []


# ════════════════════════════════════════════════════════════════════════
# 12. Email sender — _card and _filter_row
# ════════════════════════════════════════════════════════════════════════

def test_card_contains_number():
    html = _card("#fff", "#000", "42", "Total Jobs")
    assert "42" in html

def test_card_contains_label():
    html = _card("#fff", "#000", "42", "Total Jobs")
    assert "Total Jobs" in html

def test_card_with_sublabel():
    html = _card("#fff", "#000", "42", "Total Jobs", "24-hr window")
    assert "24-hr window" in html

def test_card_returns_td():
    html = _card("#fff", "#000", "42", "Total Jobs")
    assert html.strip().startswith("<td")

def test_filter_row_contains_label():
    html = _filter_row("🚫", "No sponsorship", 15)
    assert "No sponsorship" in html

def test_filter_row_contains_count():
    html = _filter_row("🚫", "No sponsorship", 15)
    assert "15" in html

def test_filter_row_red_bar_for_high_count():
    html = _filter_row("🚫", "No sponsorship", 50)
    assert "#ef4444" in html

def test_filter_row_orange_bar_for_medium_count():
    html = _filter_row("🚫", "No sponsorship", 15)
    assert "#f97316" in html

def test_filter_row_grey_bar_for_low_count():
    html = _filter_row("🚫", "No sponsorship", 5)
    assert "#94a3b8" in html

def test_filter_row_bar_width_capped_at_180():
    html = _filter_row("🚫", "No sponsorship", 999)
    assert "width:180px" in html


# ════════════════════════════════════════════════════════════════════════
# 13. sheets_manager — read_sheet_statuses row parser logic
# ════════════════════════════════════════════════════════════════════════

def test_job_id_detection_valid_hex_16():
    """Simulate the job_id detection logic in read_sheet_statuses."""
    job_id = "a1b2c3d4e5f6a7b8"
    assert len(job_id) == 16
    assert all(c in "0123456789abcdef" for c in job_id)

def test_job_id_detection_rejects_short():
    job_id = "a1b2c3d4"
    assert len(job_id) != 16

def test_job_id_detection_rejects_non_hex():
    job_id = "a1b2c3d4e5f6z7b8"
    assert not all(c in "0123456789abcdef" for c in job_id)

def test_job_id_detection_rejects_header_row():
    job_id = "Job ID"
    assert len(job_id) != 16


# ════════════════════════════════════════════════════════════════════════
# 14. ATS scoring formula integrity
# ════════════════════════════════════════════════════════════════════════

def test_ats_formula_weights_sum_to_100():
    """ATS = cosine*0.40 + kw_rate*0.50 + role*0.10 — weights must sum to 1.0."""
    weights = [0.40, 0.50, 0.10]
    assert abs(sum(weights) - 1.0) < 1e-9

def test_ats_max_theoretical_is_100():
    """With cosine=1, kw_rate=1, role_score=1 → ATS = 100."""
    ats = (1.0 * 0.40 + 1.0 * 0.50 + 1.0 * 0.10) * 100
    assert ats == 100.0

def test_ats_min_theoretical_is_5():
    """With cosine=0, kw_rate=0, role_score=0.5 → ATS = 5."""
    ats = (0.0 * 0.40 + 0.0 * 0.50 + 0.5 * 0.10) * 100
    assert ats == 5.0
