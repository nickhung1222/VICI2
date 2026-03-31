"""CLI entry point for VICI2 Taiwan News Event Study Agent.

Usage:
    # Event Study mode
    python main.py --mode event_study --stock 2330.TW \\
        --event-dates 2025-01-16,2025-04-17 --topic "TSMC法說會"

    # News Scan mode
    python main.py --mode news_scan --query "央行升息" --days 30
"""

import argparse
import sys

from dotenv import load_dotenv

load_dotenv()


def main():
    parser = argparse.ArgumentParser(
        description="VICI2: Taiwan News + Event Study Agent",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--mode",
        required=True,
        choices=["event_study", "news_scan"],
        help="Operation mode",
    )
    # event_study args
    parser.add_argument("--stock", help="Yahoo Finance symbol, e.g. '2330.TW' (required for event_study)")
    parser.add_argument("--event-dates", help="Comma-separated event dates YYYY-MM-DD (required for event_study)")
    parser.add_argument("--topic", default="台灣財經事件", help="Event description (used in report filename)")
    # news_scan args
    parser.add_argument("--query", help="News search keywords (required for news_scan)")
    parser.add_argument("--days", type=int, default=30, help="Look-back days for news_scan (default: 30)")

    args = parser.parse_args()

    from agent import event_study, news_scan

    if args.mode == "event_study":
        if not args.stock:
            parser.error("--stock is required for event_study mode")
        if not args.event_dates:
            parser.error("--event-dates is required for event_study mode")

        event_dates = [d.strip() for d in args.event_dates.split(",")]
        report_path = event_study(
            stock=args.stock,
            event_dates=event_dates,
            topic=args.topic,
        )
        if report_path:
            print(f"\n✓ Report saved: {report_path}")
        else:
            print("\n⚠ Analysis completed but no report was saved.")
            sys.exit(1)

    elif args.mode == "news_scan":
        if not args.query:
            parser.error("--query is required for news_scan mode")

        report_path = news_scan(query=args.query, days=args.days)
        if report_path:
            print(f"\n✓ Report saved: {report_path}")
        else:
            print("\n⚠ Scan completed but no report was saved.")
            sys.exit(1)


if __name__ == "__main__":
    main()
