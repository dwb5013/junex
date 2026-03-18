from __future__ import annotations

import argparse
import asyncio
import json

from app.config import Settings
from app.jobs import (
    backfill_fins_dividend,
    backfill_index_daily_bars,
    run_batch_job,
    sync_equity_master,
    sync_fins_dividend,
    sync_index_daily_bars,
    sync_margin_interest,
)
from app.logging import configure_logging


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    settings = Settings()
    configure_logging(settings.log_level)

    if args.command == "run-batch":
        result = asyncio.run(run_batch_job(settings))
    elif args.command == "sync-equity-master":
        result = {"upserted": asyncio.run(sync_equity_master(settings, code=args.code, date=args.date))}
    elif args.command == "sync-index-daily-bars":
        result = {
            "upserted": asyncio.run(
                sync_index_daily_bars(
                    settings,
                    code=args.code,
                    date=args.date,
                    from_date=args.from_date,
                    to_date=args.to_date,
                )
            )
        }
    elif args.command == "sync-fins-dividend":
        result = {
            "upserted": asyncio.run(
                sync_fins_dividend(
                    settings,
                    code=args.code,
                    date=args.date,
                    from_date=args.from_date,
                    to_date=args.to_date,
                )
            )
        }
    elif args.command == "sync-margin-interest":
        result = {
            "upserted": asyncio.run(
                sync_margin_interest(
                    settings,
                    code=args.code,
                    date=args.date,
                    from_date=args.from_date,
                    to_date=args.to_date,
                )
            )
        }
    elif args.command == "backfill-index-daily-bars":
        result = {
            "upserted": asyncio.run(
                backfill_index_daily_bars(settings, start_date=args.start_date, end_date=args.end_date)
            )
        }
    elif args.command == "backfill-fins-dividend":
        result = {
            "upserted": asyncio.run(
                backfill_fins_dividend(settings, start_date=args.start_date, end_date=args.end_date)
            )
        }
    else:
        if settings.jquants_api_key:
            result = {"upserted": asyncio.run(sync_equity_master(settings))}
        else:
            result = asyncio.run(run_batch_job(settings))

    print(json.dumps(result, ensure_ascii=True, indent=2, sort_keys=True))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="junex")
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("run-batch")

    equity_master = subparsers.add_parser("sync-equity-master")
    equity_master.add_argument("--code")
    equity_master.add_argument("--date")

    index_daily_bars = subparsers.add_parser("sync-index-daily-bars")
    _add_jquants_range_args(index_daily_bars)

    fins_dividend = subparsers.add_parser("sync-fins-dividend")
    _add_jquants_range_args(fins_dividend)

    margin_interest = subparsers.add_parser("sync-margin-interest")
    _add_jquants_range_args(margin_interest)

    backfill_index = subparsers.add_parser("backfill-index-daily-bars")
    backfill_index.add_argument("--start-date", default="2008-05-07")
    backfill_index.add_argument("--end-date")

    backfill_dividend = subparsers.add_parser("backfill-fins-dividend")
    backfill_dividend.add_argument("--start-date", default="2013-02-20")
    backfill_dividend.add_argument("--end-date")

    return parser


def _add_jquants_range_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--code")
    parser.add_argument("--date")
    parser.add_argument("--from-date")
    parser.add_argument("--to-date")


if __name__ == "__main__":
    main()
