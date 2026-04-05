"""CLI entry point for VICI2 Taiwan Earnings Call Narrative & Heat Analysis Engine.

Usage:
    # Chat mode
    python main.py --mode chat

    # Event collect mode
    python main.py --mode event_collect --stock 2330.TW \
        --event-type 法說會 --start-date 2025-04-01 --end-date 2025-04-17 \
        --stock-name 台積電 --event-date 2025-04-17 --event-key 2025Q1

    # Heat scan mode
    python main.py --mode heat_scan --stock 2330.TW \
        --event-type 法說會 --event-date 2025-04-17 --event-key 2025Q1 \
        --stock-name 台積電

    # Event report mode (primary narrative-first workflow)
    python main.py --mode event_report --stock 2330.TW \
        --event-type 法說會 --start-date 2025-04-01 --end-date 2025-04-18 \
        --event-date 2025-04-17 --event-key 2025Q1 --stock-name 台積電

    # Event Study mode (optional secondary validation)
    python main.py --mode event_study --stock 2330.TW \\
        --event-dates 2025-01-16,2025-04-17 --topic "TSMC法說會"

"""

import argparse
import sys

from dotenv import load_dotenv

load_dotenv()


def main():
    parser = argparse.ArgumentParser(
        description="VICI2: Taiwan Earnings Call Narrative & Heat Analysis Engine",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--mode",
        required=True,
        choices=["chat", "event_collect", "heat_scan", "event_report", "event_study"],
        help="Operation mode",
    )
    # event_collect args
    parser.add_argument("--event-type", help="Structured event type, e.g. '法說會' (required for event_collect)")
    parser.add_argument("--start-date", help="Collection start date YYYY-MM-DD (required for event_collect)")
    parser.add_argument("--end-date", help="Collection end date YYYY-MM-DD (required for event_collect)")
    parser.add_argument("--event-date", help="Specific event date YYYY-MM-DD (optional for event_collect)")
    parser.add_argument("--event-key", help="Recurring event key, e.g. '2025Q4' for 法說會")
    parser.add_argument("--comparison-event-date", help="Explicit prior comparable event date YYYY-MM-DD for heat_scan")
    parser.add_argument(
        "--phase",
        choices=["pre_event", "post_event", "both"],
        default="both",
        help="Heat scan phase selector: pre_event, post_event, or both (default: both)",
    )
    parser.add_argument("--stock-name", default="", help="Chinese stock name for query expansion")
    parser.add_argument("--max-results", type=int, default=12, help="Maximum collected records for event_collect")
    parser.add_argument(
        "--include-event-study",
        action="store_true",
        help="Include optional deterministic event study validation in event_report",
    )
    # event_study args
    parser.add_argument("--stock", help="Yahoo Finance symbol, e.g. '2330.TW' (required for formal modes)")
    parser.add_argument("--event-dates", help="Comma-separated event dates YYYY-MM-DD (required for event_study)")
    parser.add_argument("--topic", default="台灣法說會事件", help="Report topic (used in report filename)")
    args = parser.parse_args()

    from agent import event_study
    from chat_cli import run_chat_mode
    from pipeline import event_collect, event_report, heat_scan

    if args.mode == "chat":
        run_chat_mode()

    elif args.mode == "event_collect":
        if not args.stock:
            parser.error("--stock is required for event_collect mode")
        if not args.event_type:
            parser.error("--event-type is required for event_collect mode")
        if not args.start_date:
            parser.error("--start-date is required for event_collect mode")
        if not args.end_date:
            parser.error("--end-date is required for event_collect mode")

        output_path = event_collect(
            stock=args.stock,
            event_type=args.event_type,
            start_date=args.start_date,
            end_date=args.end_date,
            stock_name=args.stock_name,
            event_date=args.event_date or "",
            event_key=args.event_key or "",
            max_results=args.max_results,
        )
        print(f"\n✓ Event records saved: {output_path}")

    elif args.mode == "heat_scan":
        if not args.stock:
            parser.error("--stock is required for heat_scan mode")
        if not args.event_type:
            parser.error("--event-type is required for heat_scan mode")
        if not args.event_date:
            parser.error("--event-date is required for heat_scan mode")

        output_path = heat_scan(
            stock=args.stock,
            event_type=args.event_type,
            event_date=args.event_date,
            stock_name=args.stock_name,
            event_key=args.event_key or "",
            comparison_event_date=args.comparison_event_date or "",
            max_results=args.max_results,
            phase=args.phase,
        )
        print(f"\n✓ Heat analysis saved: {output_path}")

    elif args.mode == "event_report":
        if not args.stock:
            parser.error("--stock is required for event_report mode")
        if not args.event_type:
            parser.error("--event-type is required for event_report mode")
        if not args.start_date:
            parser.error("--start-date is required for event_report mode")
        if not args.end_date:
            parser.error("--end-date is required for event_report mode")
        if not args.event_date:
            parser.error("--event-date is required for event_report mode")

        output_paths = event_report(
            stock=args.stock,
            event_type=args.event_type,
            start_date=args.start_date,
            end_date=args.end_date,
            event_date=args.event_date,
            stock_name=args.stock_name,
            event_key=args.event_key or "",
            comparison_event_date=args.comparison_event_date or "",
            max_results=args.max_results,
            include_event_study=args.include_event_study,
            topic=args.topic,
        )
        print(f"\n✓ Event report JSON saved: {output_paths['json_path']}")
        print(f"✓ Event report Markdown saved: {output_paths['markdown_path']}")

    elif args.mode == "event_study":
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

if __name__ == "__main__":
    main()
