"""Unit tests for the event study calculation engine."""

import numpy as np
import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from tools.event_study import run_event_study


def _make_synthetic_data(n_days=300, seed=42):
    """Generate synthetic market + stock returns for testing."""
    rng = np.random.default_rng(seed)
    market_returns = rng.normal(0.0003, 0.01, n_days).tolist()
    # Stock: highly correlated with market (beta ~1.2) + small alpha
    stock_returns = [0.0001 + 1.2 * m + rng.normal(0, 0.005) for m in market_returns]
    # Generate business days starting from 2023-01-02
    from datetime import datetime, timedelta
    start = datetime(2023, 1, 2)
    dates = []
    d = start
    while len(dates) < n_days:
        if d.weekday() < 5:  # Mon-Fri
            dates.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)
    return stock_returns, market_returns, dates


class TestRunEventStudy:
    def test_basic_single_event(self):
        stock_r, market_r, dates = _make_synthetic_data(300)
        # Pick event date at index ~200 (well within data)
        event_date = dates[200]
        result = run_event_study(
            stock_returns=stock_r,
            market_returns=market_r,
            dates=dates,
            event_dates=[event_date],
        )
        assert result["n_events"] == 1
        assert len(result["relative_days"]) == 11  # [-5, +5]
        assert len(result["avg_car"]) == 11
        assert result["relative_days"][5] == 0  # day 0 is in the middle

    def test_multiple_events(self):
        stock_r, market_r, dates = _make_synthetic_data(300)
        # Pick 3 event dates spread across the data
        event_dates = [dates[150], dates[200], dates[250]]
        result = run_event_study(
            stock_returns=stock_r,
            market_returns=market_r,
            dates=dates,
            event_dates=event_dates,
        )
        assert result["n_events"] == 3
        assert len(result["individual_cars"]) == 3
        # Each individual CAR should have 11 values
        for car in result["individual_cars"]:
            assert len(car) == 11

    def test_car_starts_near_zero(self):
        """For a stock with beta~1, AR should be close to zero on average."""
        rng = np.random.default_rng(99)
        n = 300
        market_r = rng.normal(0.0003, 0.01, n).tolist()
        # Perfect market model (no abnormal returns)
        stock_r = [1.0 * m for m in market_r]
        from datetime import datetime, timedelta
        dates = []
        d = datetime(2023, 1, 2)
        while len(dates) < n:
            if d.weekday() < 5:
                dates.append(d.strftime("%Y-%m-%d"))
            d += timedelta(days=1)

        result = run_event_study(
            stock_returns=stock_r,
            market_returns=market_r,
            dates=dates,
            event_dates=[dates[200]],
        )
        avg_car = result["avg_car"]
        # CAR should be very close to zero for a perfect market model
        assert all(abs(c) < 0.05 for c in avg_car), f"CAR values unexpectedly large: {avg_car}"

    def test_insufficient_data_skipped(self):
        """Events too early in the series should be skipped gracefully."""
        stock_r, market_r, dates = _make_synthetic_data(300)
        # Event at index 10 — not enough history for estimation window
        early_event = dates[10]
        result = run_event_study(
            stock_returns=stock_r,
            market_returns=market_r,
            dates=dates,
            event_dates=[early_event],
        )
        assert result["n_events"] == 0
        assert len(result["skipped_events"]) == 1

    def test_custom_window(self):
        stock_r, market_r, dates = _make_synthetic_data(300)
        result = run_event_study(
            stock_returns=stock_r,
            market_returns=market_r,
            dates=dates,
            event_dates=[dates[200]],
            event_window_pre=2,
            event_window_post=3,
        )
        assert len(result["relative_days"]) == 6  # [-2, -1, 0, 1, 2, 3]
        assert result["relative_days"][0] == -2
        assert result["relative_days"][-1] == 3
