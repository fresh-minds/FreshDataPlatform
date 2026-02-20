from pathlib import Path


def test_makefile_exposes_bootstrap_all_alias() -> None:
    makefile = (Path(__file__).resolve().parents[2] / "Makefile").read_text()
    assert "bootstrap_all: bootstrap-all" in makefile


def test_bootstrap_script_verifies_keycloak_resources() -> None:
    script = (Path(__file__).resolve().parents[2] / "scripts" / "platform" / "bootstrap_all.sh").read_text()
    assert "verify_keycloak_resources.py" in script
    assert "--skip-keycloak-check" in script


def test_bootstrap_script_ensures_job_market_sources_before_dbt() -> None:
    script = (Path(__file__).resolve().parents[2] / "scripts" / "platform" / "bootstrap_all.sh").read_text()
    ensure_idx = script.find("ensure_job_market_source_tables.py")
    dbt_idx = script.find("scripts/pipeline/run_dbt_parallel.sh")

    assert ensure_idx != -1
    assert dbt_idx != -1
    assert ensure_idx < dbt_idx
