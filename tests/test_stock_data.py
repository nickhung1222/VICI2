"""Unit tests for stock data fetcher."""

import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))


class TestFetchStockData:
    def test_basic_tsmc_fetch(self):
        """Integration test: fetch TSMC data from yfinance."""
        from tools.stock_data import fetch_stock_data
        result = fetch_stock_data("2330.TW", "2024-01-01", "2024-03-31")

        assert "symbol" in result
        assert result["symbol"] == "2330.TW"
        assert len(result["dates"]) > 0
        assert len(result["stock_returns"]) == len(result["dates"])
        assert len(result["market_returns"]) == len(result["dates"])
        assert len(result["stock_close"]) == len(result["dates"])

    def test_returns_are_reasonable(self):
        """Daily returns should be in a reasonable range (not >50%)."""
        from tools.stock_data import fetch_stock_data
        result = fetch_stock_data("2330.TW", "2024-01-01", "2024-06-30")

        for r in result["stock_returns"]:
            assert -0.5 < r < 0.5, f"Unreasonable return: {r}"

    def test_invalid_symbol_raises(self):
        """Invalid symbol should raise RuntimeError or ValueError."""
        from tools.stock_data import fetch_stock_data
        with pytest.raises((RuntimeError, ValueError)):
            fetch_stock_data("INVALID_SYMBOL_XYZ.TW", "2024-01-01", "2024-03-31")
