"""Stock price data fetcher for Taiwan-listed stocks.

Uses yfinance as primary source (Taiwan stocks use .TW suffix).
Falls back to TWSE open data CSV for reliability.
"""

import time
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf


def fetch_stock_data(symbol: str, start_date: str, end_date: str) -> dict:
    """Fetch historical stock price data and TAIEX benchmark.

    Args:
        symbol: Yahoo Finance symbol, e.g. '2330.TW' for TSMC
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format

    Returns:
        dict with keys: symbol, dates, stock_returns, market_returns, stock_close, market_close
    """
    # Extend start_date back by ~200 days to ensure enough data for estimation window
    start_dt = datetime.strptime(start_date, "%Y-%m-%d") - timedelta(days=200)
    extended_start = start_dt.strftime("%Y-%m-%d")

    try:
        stock_df = yf.download(symbol, start=extended_start, end=end_date, auto_adjust=True, progress=False)
        market_df = yf.download("^TWII", start=extended_start, end=end_date, auto_adjust=True, progress=False)
    except Exception as e:
        raise RuntimeError(f"yfinance download failed for {symbol}: {e}") from e

    if stock_df.empty:
        raise ValueError(f"No price data returned for {symbol}. Check symbol and date range.")
    if market_df.empty:
        raise ValueError("No price data returned for TAIEX (^TWII).")

    # Extract close prices (handle MultiIndex columns from yfinance)
    stock_close = stock_df["Close"]
    market_close = market_df["Close"]

    if isinstance(stock_close, pd.DataFrame):
        stock_close = stock_close.iloc[:, 0]
    if isinstance(market_close, pd.DataFrame):
        market_close = market_close.iloc[:, 0]

    # Align on common trading dates
    common_dates = stock_close.index.intersection(market_close.index)
    stock_close = stock_close.loc[common_dates]
    market_close = market_close.loc[common_dates]

    # Calculate daily log returns
    stock_returns = stock_close.pct_change().dropna()
    market_returns = market_close.pct_change().dropna()

    common_return_dates = stock_returns.index.intersection(market_returns.index)
    stock_returns = stock_returns.loc[common_return_dates]
    market_returns = market_returns.loc[common_return_dates]

    return {
        "symbol": symbol,
        "dates": [d.strftime("%Y-%m-%d") for d in common_return_dates],
        "stock_returns": stock_returns.values.tolist(),
        "market_returns": market_returns.values.tolist(),
        "stock_close": stock_close.loc[common_return_dates].values.tolist(),
        "market_close": market_close.loc[common_return_dates].values.tolist(),
    }


def get_trading_calendar(symbol: str, start_date: str, end_date: str) -> list[str]:
    """Return list of trading dates for a given symbol."""
    df = yf.download(symbol, start=start_date, end=end_date, auto_adjust=True, progress=False)
    return [d.strftime("%Y-%m-%d") for d in df.index]
