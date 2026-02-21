from __future__ import annotations

import pytest

import scripts.pipeline.ensure_job_market_source_tables as ensure_job_market_source_tables


def test_main_creates_tables_and_closes_connection(monkeypatch):
    calls = {"ensure": 0, "close": 0}

    class DummyConnection:
        def close(self):
            calls["close"] += 1

    def fake_connect():
        return DummyConnection()

    def fake_ensure(conn):
        assert isinstance(conn, DummyConnection)
        calls["ensure"] += 1

    monkeypatch.setattr(ensure_job_market_source_tables, "_connect_warehouse", fake_connect)
    monkeypatch.setattr(ensure_job_market_source_tables, "ensure_tables", fake_ensure)

    rc = ensure_job_market_source_tables.main()

    assert rc == 0
    assert calls["ensure"] == 1
    assert calls["close"] == 1


def test_main_closes_connection_even_when_ensure_raises(monkeypatch):
    """Connection must be closed even if ensure_tables raises."""
    calls = {"close": 0}

    class DummyConnection:
        def close(self):
            calls["close"] += 1

    def fake_ensure(_conn):
        raise RuntimeError("db error")

    monkeypatch.setattr(ensure_job_market_source_tables, "_connect_warehouse", lambda: DummyConnection())
    monkeypatch.setattr(ensure_job_market_source_tables, "ensure_tables", fake_ensure)

    with pytest.raises(RuntimeError, match="db error"):
        ensure_job_market_source_tables.main()

    assert calls["close"] == 1, "connection.close() must be called even after an exception"
