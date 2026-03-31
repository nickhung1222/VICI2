"""Chart generation for event study results.

Generates CAR (Cumulative Abnormal Return) charts with confidence intervals.
Saves PNG files to outputs/charts/.
"""

import os
from datetime import datetime
from pathlib import Path

import matplotlib
matplotlib.use("Agg")  # non-interactive backend
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import numpy as np


def _setup_chinese_font():
    """Configure matplotlib to use a CJK font available on macOS/Linux."""
    candidate_fonts = [
        "PingFang TC",
        "Heiti TC",
        "Apple LiGothic",
        "Arial Unicode MS",
        "WenQuanYi Zen Hei",
        "Noto Sans CJK TC",
    ]
    available = {f.name for f in fm.fontManager.ttflist}
    for font in candidate_fonts:
        if font in available:
            plt.rcParams["font.sans-serif"] = [font, "DejaVu Sans"]
            break
    else:
        # Fallback: use system default (may not render Chinese)
        plt.rcParams["font.sans-serif"] = ["DejaVu Sans"]
    plt.rcParams["axes.unicode_minus"] = False


def generate_car_chart(car_data: dict, title: str, symbol: str = "") -> str:
    """Generate average CAR chart and save to outputs/charts/.

    Args:
        car_data: Output from run_event_study()
        title: Chart title (can include Chinese characters)
        symbol: Stock symbol for filename (optional)

    Returns:
        Absolute path to the saved PNG file
    """
    _setup_chinese_font()

    relative_days = car_data["relative_days"]
    avg_car = [x * 100 for x in car_data["avg_car"]]  # convert to %
    std_error = car_data.get("std_error", [])
    n_events = car_data.get("n_events", 0)
    individual_cars = car_data.get("individual_cars", [])

    fig, ax = plt.subplots(figsize=(10, 6))

    # Plot individual event CAR lines (light, for context)
    for car in individual_cars:
        car_pct = [x * 100 for x in car]
        ax.plot(relative_days, car_pct, color="#AAAAAA", linewidth=0.8, alpha=0.4)

    # Confidence interval (if multiple events)
    has_ci = (
        std_error
        and n_events > 1
        and not all(np.isnan(x) for x in std_error)
    )
    if has_ci:
        se_pct = [x * 100 for x in std_error]
        upper = [c + 1.96 * s for c, s in zip(avg_car, se_pct)]
        lower = [c - 1.96 * s for c, s in zip(avg_car, se_pct)]
        ax.fill_between(relative_days, lower, upper, alpha=0.2, color="#2196F3", label="95% CI")

    # Average CAR line
    ax.plot(relative_days, avg_car, color="#1565C0", linewidth=2.5, marker="o", markersize=5, label=f"Average CAR (n={n_events})")

    # Event day vertical line
    ax.axvline(x=0, color="#E53935", linewidth=1.5, linestyle="--", alpha=0.8, label="Event Day (t=0)")
    ax.axhline(y=0, color="#555555", linewidth=0.8, linestyle="-", alpha=0.5)

    ax.set_xlabel("Event Day (Relative)", fontsize=12)
    ax.set_ylabel("CAR (%)", fontsize=12)
    ax.set_title(title, fontsize=14, fontweight="bold", pad=15)
    ax.legend(loc="upper left", fontsize=10)
    ax.set_xticks(relative_days)
    ax.grid(True, alpha=0.3)

    # Annotate final CAR value
    if avg_car:
        final_car = avg_car[-1]
        ax.annotate(
            f"CAR={final_car:+.2f}%",
            xy=(relative_days[-1], final_car),
            xytext=(relative_days[-1] - 1.5, final_car + (0.1 if final_car >= 0 else -0.1)),
            fontsize=9,
            color="#1565C0",
        )

    plt.tight_layout()

    # Save
    output_dir = Path(__file__).parent.parent / "outputs" / "charts"
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_symbol = symbol.replace(".", "_") if symbol else "stock"
    filename = f"car_{safe_symbol}_{timestamp}.png"
    filepath = output_dir / filename

    fig.savefig(filepath, dpi=150, bbox_inches="tight")
    plt.close(fig)

    return str(filepath)
