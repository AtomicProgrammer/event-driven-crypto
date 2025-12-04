"""
针对数据拉取与入库流程的集成测试（使用模拟客户端）。
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from src import ingest


class DummyClient:
    def __init__(self, klines):
        self.klines = klines
        self.calls = []

    def get_historical_klines(self, **kwargs):
        self.calls.append(kwargs)
        return self.klines


def test_ingest_eth_data_inserts_rows(tmp_path: Path):
    db_path = tmp_path / "market.db"
    dummy_data = [
        ["1696118400000", "3000", "3100", "2950", "3050", "123.4", "1696122000000", "400000", "800", "60.0", "200000", "0"],
        ["1696122000000", "3050", "3150", "3000", "3100", "200.0", "1696125600000", "500000", "900", "70.0", "250000", "0"],
    ]
    inserted = ingest.ingest_eth_data(
        start="2025-10-01",
        end="2025-10-02",
        interval="1h",
        db_path=str(db_path),
        client=DummyClient(dummy_data),
    )

    assert inserted == len(dummy_data)
    with sqlite3.connect(db_path) as conn:
        (count,) = conn.execute("SELECT COUNT(*) FROM eth_klines").fetchone()
        assert count == len(dummy_data)


def test_ingest_eth_data_with_invalid_date(tmp_path: Path):
    db_path = tmp_path / "market.db"
    client = DummyClient([])
    with pytest.raises(ValueError):
        ingest.ingest_eth_data(
            start="invalid-date",
            end="2025-10-02",
            interval="1h",
            db_path=str(db_path),
            client=client,
        )

