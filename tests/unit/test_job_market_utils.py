from pipelines.job_market_nl.utils import extract_skills_from_text, normalize_salary_to_year


def test_extract_skills_from_text():
    text = "Looking for a Python data engineer with AWS and SQL."
    skills = extract_skills_from_text(text)
    assert "python" in skills
    assert "aws" in skills
    assert "sql" in skills


def test_normalize_salary_to_year_hourly():
    result = normalize_salary_to_year(50, 80, "hour")
    assert result.min_eur_year == 50 * 40 * 52
    assert result.max_eur_year == 80 * 40 * 52
