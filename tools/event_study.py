"""Event Study engine: Abnormal Return (AR) and Cumulative Abnormal Return (CAR).

Implements the standard market model approach (MacKinlay 1997):
  R_i = alpha + beta * R_m + epsilon

Windows:
  Estimation window: [-130, -11] relative to event date (120 trading days)
  Gap: [-10, -6] — buffer to avoid event contamination
  Event window: [-5, +5] relative to event date (11 trading days)
"""

import numpy as np
import pandas as pd
from scipy import stats


def run_event_study(
    stock_returns: list[float],
    market_returns: list[float],
    dates: list[str],
    event_dates: list[str],
    estimation_window: int = 120,
    event_window_pre: int = 5,
    event_window_post: int = 5,
) -> dict:
    """Run event study for a list of event dates.

    Args:
        stock_returns: Daily stock returns (aligned with dates)
        market_returns: Daily market (TAIEX) returns (aligned with dates)
        dates: List of date strings YYYY-MM-DD corresponding to return arrays
        event_dates: Event dates to analyze (YYYY-MM-DD format)
        estimation_window: Number of trading days for OLS estimation (default 120)
        event_window_pre: Days before event to include in event window (default 5)
        event_window_post: Days after event to include in event window (default 5)

    Returns:
        dict with avg_car, std_error, t_stats, individual_cars, relative_days, n_events, skipped_events
    """
    returns_series = pd.Series(stock_returns, index=pd.to_datetime(dates))
    market_series = pd.Series(market_returns, index=pd.to_datetime(dates))

    all_cars = []
    all_ars = []
    skipped = []
    window_size = event_window_pre + event_window_post + 1

    for event_date_str in event_dates:
        event_dt = pd.to_datetime(event_date_str)

        # Find the nearest trading day on or after the event date
        available = returns_series.index[returns_series.index >= event_dt]
        if len(available) == 0:
            skipped.append({"date": event_date_str, "reason": "event date after all available data"})
            continue
        event_idx = returns_series.index.get_loc(available[0])

        # Estimation window: [event_idx - 130, event_idx - 11]
        gap = 10  # buffer days between estimation and event window
        est_end = event_idx - gap
        est_start = est_end - estimation_window

        if est_start < 0:
            skipped.append({"date": event_date_str, "reason": f"insufficient data for estimation window (need {estimation_window + gap + event_window_pre} days before event)"})
            continue

        # Event window: [event_idx - pre, event_idx + post]
        ev_start = event_idx - event_window_pre
        ev_end = event_idx + event_window_post + 1  # +1 for slice

        if ev_end > len(returns_series):
            skipped.append({"date": event_date_str, "reason": "insufficient data after event for event window"})
            continue

        # OLS estimation: R_stock = alpha + beta * R_market
        est_stock = returns_series.iloc[est_start:est_end].values
        est_market = market_series.iloc[est_start:est_end].values

        slope, intercept, r_value, p_value, std_err = stats.linregress(est_market, est_stock)
        alpha, beta = intercept, slope

        # Calculate abnormal returns in event window
        ev_stock = returns_series.iloc[ev_start:ev_end].values
        ev_market = market_series.iloc[ev_start:ev_end].values

        if len(ev_stock) < window_size:
            skipped.append({"date": event_date_str, "reason": f"incomplete event window: got {len(ev_stock)} days, need {window_size}"})
            continue

        expected_returns = alpha + beta * ev_market
        ar = ev_stock - expected_returns
        car = np.cumsum(ar)

        all_ars.append(ar)
        all_cars.append(car)

    n_events = len(all_cars)
    relative_days = list(range(-event_window_pre, event_window_post + 1))

    if n_events == 0:
        return {
            "n_events": 0,
            "skipped_events": skipped,
            "relative_days": relative_days,
            "avg_car": [0.0] * window_size,
            "avg_ar": [0.0] * window_size,
            "std_error": [0.0] * window_size,
            "t_stats": [0.0] * window_size,
            "individual_cars": [],
            "error": "No events could be analyzed. Check that event dates fall within the data range with sufficient history.",
        }

    cars_array = np.array(all_cars)  # shape: (n_events, window_size)
    ars_array = np.array(all_ars)

    avg_car = cars_array.mean(axis=0)
    avg_ar = ars_array.mean(axis=0)

    if n_events == 1:
        # Single event: use estimation-window residual std as the benchmark
        std_error = np.full(window_size, np.nan)
        t_stats = np.full(window_size, np.nan)
    else:
        # Cross-sectional t-test (Boehmer et al. 1991 approach)
        std_error = cars_array.std(axis=0, ddof=1) / np.sqrt(n_events)
        t_stats = avg_car / (std_error + 1e-10)

    return {
        "n_events": n_events,
        "skipped_events": skipped,
        "relative_days": relative_days,
        "avg_car": avg_car.tolist(),
        "avg_ar": avg_ar.tolist(),
        "std_error": [float(x) for x in std_error],
        "t_stats": [float(x) for x in t_stats],
        "individual_cars": [c.tolist() for c in all_cars],
        "event_dates_used": [d for d in event_dates if d not in [s["date"] for s in skipped]],
    }
