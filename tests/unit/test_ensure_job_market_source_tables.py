from __future__ import annotations

import scripts.ensure_job_market_source_tables as ensure_job_market_source_tables


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
