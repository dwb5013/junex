from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from app.config import Settings
from app.jobs import (
    backfill_fins_dividend,
    backfill_index_daily_bars,
    build_flow_structure_feature_table,
    build_fundamental_event_feature_table,
    build_llm_prediction_eval_table,
    build_market_industry_linkage_feature_table,
    build_next_day_label_table,
    build_price_action_feature_table,
    export_stock_factor_snapshot,
    finalize_llm_eval_bundle,
    finalize_openai_batch,
    import_llm_prediction,
    prepare_llm_eval_bundle,
    prepare_openai_batch_range,
    run_daily_workflow,
    run_batch_job,
    run_llm_eval_auto,
    run_llm_eval_auto_range,
    submit_openai_batch,
    summarize_llm_prediction_eval,
    sync_reference_market_segments,
    sync_reference_sector17,
    sync_reference_sector33,
    sync_earnings_calendar,
    sync_equity_daily_bars,
    sync_equity_master,
    sync_fins_summary,
    sync_fins_dividend,
    sync_index_daily_bars,
    sync_margin_alert,
    sync_market_breakdown,
    sync_margin_interest,
    sync_market_calendar,
    sync_short_sale_report,
    sync_short_ratio,
    sync_topix_daily_bars,
)
from app.logging import configure_logging


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    settings = Settings()
    configure_logging(settings.log_level)

    if args.command == "run-batch":
        result = asyncio.run(run_batch_job(settings))
    elif args.command == "dailywork":
        result = asyncio.run(run_daily_workflow(settings, target_date=args.date))
    elif args.command == "sync-reference-market-segments":
        result = {"upserted": asyncio.run(sync_reference_market_segments(settings))}
    elif args.command == "sync-reference-sector17":
        result = {"upserted": asyncio.run(sync_reference_sector17(settings))}
    elif args.command == "sync-reference-sector33":
        result = {"upserted": asyncio.run(sync_reference_sector33(settings))}
    elif args.command == "sync-equity-master":
        result = {"upserted": asyncio.run(sync_equity_master(settings, code=args.code, date=args.date))}
    elif args.command == "sync-equity-daily-bars":
        result = {
            "upserted": asyncio.run(
                sync_equity_daily_bars(
                    settings,
                    code=args.code,
                    date=args.date,
                    from_date=args.from_date,
                    to_date=args.to_date,
                )
            )
        }
    elif args.command == "sync-market-calendar":
        result = {
            "upserted": asyncio.run(
                sync_market_calendar(
                    settings,
                    holiday_division=args.holiday_division,
                    from_date=args.from_date,
                    to_date=args.to_date,
                )
            )
        }
    elif args.command == "sync-market-breakdown":
        result = {
            "upserted": asyncio.run(
                sync_market_breakdown(
                    settings,
                    code=args.code,
                    date=args.date,
                    from_date=args.from_date,
                    to_date=args.to_date,
                )
            )
        }
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
    elif args.command == "sync-topix-daily-bars":
        result = {
            "upserted": asyncio.run(
                sync_topix_daily_bars(
                    settings,
                    date=args.date,
                    from_date=args.from_date,
                    to_date=args.to_date,
                )
            )
        }
    elif args.command == "sync-fins-summary":
        result = {
            "upserted": asyncio.run(
                sync_fins_summary(
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
    elif args.command == "sync-margin-alert":
        result = {
            "upserted": asyncio.run(
                sync_margin_alert(
                    settings,
                    code=args.code,
                    date=args.date,
                    from_date=args.from_date,
                    to_date=args.to_date,
                )
            )
        }
    elif args.command == "sync-short-ratio":
        result = {
            "upserted": asyncio.run(
                sync_short_ratio(
                    settings,
                    s33=args.s33,
                    date=args.date,
                    from_date=args.from_date,
                    to_date=args.to_date,
                )
            )
        }
    elif args.command == "sync-short-sale-report":
        result = {
            "upserted": asyncio.run(
                sync_short_sale_report(
                    settings,
                    code=args.code,
                    disclosed_date=args.disclosed_date,
                    disclosed_date_from=args.disclosed_date_from,
                    disclosed_date_to=args.disclosed_date_to,
                    calculated_date=args.calculated_date,
                )
            )
        }
    elif args.command == "sync-earnings-calendar":
        result = {"upserted": asyncio.run(sync_earnings_calendar(settings))}
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
    elif args.command == "build-price-action-features":
        result = {"row_count": asyncio.run(build_price_action_feature_table(settings))}
    elif args.command == "build-market-industry-linkage-features":
        result = {"row_count": asyncio.run(build_market_industry_linkage_feature_table(settings))}
    elif args.command == "build-flow-structure-features":
        result = {"row_count": asyncio.run(build_flow_structure_feature_table(settings))}
    elif args.command == "build-fundamental-event-features":
        result = {"row_count": asyncio.run(build_fundamental_event_feature_table(settings))}
    elif args.command == "build-next-day-labels":
        result = {"row_count": asyncio.run(build_next_day_label_table(settings))}
    elif args.command == "import-llm-prediction":
        result = {
            "upserted": asyncio.run(
                import_llm_prediction(
                    settings,
                    input_path=args.input,
                    code=args.code,
                    provider=args.provider,
                    model=args.model,
                    prompt_version=args.prompt_version,
                )
            )
        }
    elif args.command == "build-llm-prediction-eval":
        result = {"row_count": asyncio.run(build_llm_prediction_eval_table(settings))}
    elif args.command == "prepare-llm-eval":
        result = asyncio.run(
            prepare_llm_eval_bundle(
                settings,
                code=args.code,
                target_date=args.date,
                window_trading_days=args.window_trading_days,
                output_dir=args.output_dir,
            )
        )
    elif args.command == "finalize-llm-eval":
        result = asyncio.run(
            finalize_llm_eval_bundle(
                settings,
                bundle_dir=args.bundle_dir,
                provider=args.provider,
                model=args.model,
                prompt_version=args.prompt_version,
                input_path=args.input,
            )
        )
    elif args.command == "summarize-llm-prediction-eval":
        result = asyncio.run(
            summarize_llm_prediction_eval(
                settings,
                provider=args.provider,
                model=args.model,
                prompt_version=args.prompt_version,
                code=args.code,
            )
        )
    elif args.command == "run-llm-eval-auto":
        result = asyncio.run(
            run_llm_eval_auto(
                settings,
                code=args.code,
                provider=args.provider,
                model=args.model,
                target_date=args.date,
                prompt_version=args.prompt_version,
                window_trading_days=args.window_trading_days,
                output_dir=args.output_dir,
                reasoning_effort=args.reasoning_effort,
            )
        )
    elif args.command == "run-llm-eval-auto-range":
        result = asyncio.run(
            run_llm_eval_auto_range(
                settings,
                code=args.code,
                provider=args.provider,
                model=args.model,
                start_date=args.from_date,
                end_date=args.to_date,
                prompt_version=args.prompt_version,
                window_trading_days=args.window_trading_days,
                output_dir=args.output_dir,
                overwrite_existing=args.overwrite_existing,
                reasoning_effort=args.reasoning_effort,
            )
        )
    elif args.command == "prepare-openai-batch-range":
        result = asyncio.run(
            prepare_openai_batch_range(
                settings,
                code=args.code,
                model=args.model,
                start_date=args.from_date,
                end_date=args.to_date,
                prompt_version=args.prompt_version,
                window_trading_days=args.window_trading_days,
                output_dir=args.output_dir,
                overwrite_existing=args.overwrite_existing,
                reasoning_effort=args.reasoning_effort,
            )
        )
    elif args.command == "submit-openai-batch":
        result = asyncio.run(
            submit_openai_batch(
                settings,
                batch_dir=args.batch_dir,
                completion_window=args.completion_window,
            )
        )
    elif args.command == "finalize-openai-batch":
        result = asyncio.run(
            finalize_openai_batch(
                settings,
                batch_dir=args.batch_dir,
                batch_id=args.batch_id,
            )
        )
    elif args.command == "export-stock-factors":
        result = asyncio.run(
            export_stock_factor_snapshot(
                settings,
                code=args.code,
                target_date=args.date,
            )
        )
        status = result.get("status")
        if status in {"not_trading_day", "no_data"}:
            raise SystemExit(1)
        output_dir = Path(args.output_dir or ".").expanduser()
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / f"{result['code']}_{result['trade_date']}.json"
        output_file.write_text(
            json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        print(str(output_file))
        return
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
    dailywork = subparsers.add_parser("dailywork")
    dailywork.add_argument("--date")
    subparsers.add_parser("sync-reference-market-segments")
    subparsers.add_parser("sync-reference-sector17")
    subparsers.add_parser("sync-reference-sector33")

    equity_master = subparsers.add_parser("sync-equity-master")
    equity_master.add_argument("--code")
    equity_master.add_argument("--date")

    equity_daily_bars = subparsers.add_parser("sync-equity-daily-bars")
    _add_jquants_range_args(equity_daily_bars)

    market_calendar = subparsers.add_parser("sync-market-calendar")
    market_calendar.add_argument("--holiday-division")
    market_calendar.add_argument("--from-date")
    market_calendar.add_argument("--to-date")

    market_breakdown = subparsers.add_parser("sync-market-breakdown")
    _add_jquants_range_args(market_breakdown)

    index_daily_bars = subparsers.add_parser("sync-index-daily-bars")
    _add_jquants_range_args(index_daily_bars)

    topix_daily_bars = subparsers.add_parser("sync-topix-daily-bars")
    topix_daily_bars.add_argument("--date")
    topix_daily_bars.add_argument("--from-date")
    topix_daily_bars.add_argument("--to-date")

    fins_summary = subparsers.add_parser("sync-fins-summary")
    _add_jquants_range_args(fins_summary)

    fins_dividend = subparsers.add_parser("sync-fins-dividend")
    _add_jquants_range_args(fins_dividend)

    margin_interest = subparsers.add_parser("sync-margin-interest")
    _add_jquants_range_args(margin_interest)

    margin_alert = subparsers.add_parser("sync-margin-alert")
    _add_jquants_range_args(margin_alert)

    short_ratio = subparsers.add_parser("sync-short-ratio")
    short_ratio.add_argument("--s33")
    short_ratio.add_argument("--date")
    short_ratio.add_argument("--from-date")
    short_ratio.add_argument("--to-date")

    short_sale_report = subparsers.add_parser("sync-short-sale-report")
    short_sale_report.add_argument("--code")
    short_sale_report.add_argument("--disclosed-date")
    short_sale_report.add_argument("--disclosed-date-from")
    short_sale_report.add_argument("--disclosed-date-to")
    short_sale_report.add_argument("--calculated-date")

    subparsers.add_parser("sync-earnings-calendar")

    backfill_index = subparsers.add_parser("backfill-index-daily-bars")
    backfill_index.add_argument("--start-date", default="2008-05-07")
    backfill_index.add_argument("--end-date")

    backfill_dividend = subparsers.add_parser("backfill-fins-dividend")
    backfill_dividend.add_argument("--start-date", default="2013-02-20")
    backfill_dividend.add_argument("--end-date")

    subparsers.add_parser("build-price-action-features")
    subparsers.add_parser("build-market-industry-linkage-features")
    subparsers.add_parser("build-flow-structure-features")
    subparsers.add_parser("build-fundamental-event-features")
    subparsers.add_parser("build-next-day-labels")
    import_llm_prediction_parser = subparsers.add_parser("import-llm-prediction")
    import_llm_prediction_parser.add_argument("--input", required=True)
    import_llm_prediction_parser.add_argument("--code", required=True)
    import_llm_prediction_parser.add_argument("--provider", required=True)
    import_llm_prediction_parser.add_argument("--model", required=True)
    import_llm_prediction_parser.add_argument("--prompt-version", default="v1")
    subparsers.add_parser("build-llm-prediction-eval")
    prepare_llm_eval = subparsers.add_parser("prepare-llm-eval")
    prepare_llm_eval.add_argument("--code", required=True)
    prepare_llm_eval.add_argument("--date")
    prepare_llm_eval.add_argument("--window-trading-days", type=int, default=20)
    prepare_llm_eval.add_argument("--output-dir")
    finalize_llm_eval = subparsers.add_parser("finalize-llm-eval")
    finalize_llm_eval.add_argument("--bundle-dir", required=True)
    finalize_llm_eval.add_argument("--provider", required=True)
    finalize_llm_eval.add_argument("--model", required=True)
    finalize_llm_eval.add_argument("--prompt-version", default="v1")
    finalize_llm_eval.add_argument("--input")
    summarize_llm_eval = subparsers.add_parser("summarize-llm-prediction-eval")
    summarize_llm_eval.add_argument("--provider")
    summarize_llm_eval.add_argument("--model")
    summarize_llm_eval.add_argument("--prompt-version")
    summarize_llm_eval.add_argument("--code")
    run_llm_eval_auto_parser = subparsers.add_parser("run-llm-eval-auto")
    run_llm_eval_auto_parser.add_argument("--code", required=True)
    run_llm_eval_auto_parser.add_argument("--provider", required=True)
    run_llm_eval_auto_parser.add_argument("--model", required=True)
    run_llm_eval_auto_parser.add_argument("--date")
    run_llm_eval_auto_parser.add_argument("--prompt-version", default="v1")
    run_llm_eval_auto_parser.add_argument("--window-trading-days", type=int, default=20)
    run_llm_eval_auto_parser.add_argument("--output-dir")
    run_llm_eval_auto_parser.add_argument("--reasoning-effort", choices=["low", "medium", "high", "xhigh"])
    run_llm_eval_auto_range_parser = subparsers.add_parser("run-llm-eval-auto-range")
    run_llm_eval_auto_range_parser.add_argument("--code", required=True)
    run_llm_eval_auto_range_parser.add_argument("--provider", required=True)
    run_llm_eval_auto_range_parser.add_argument("--model", required=True)
    run_llm_eval_auto_range_parser.add_argument("--from-date", required=True)
    run_llm_eval_auto_range_parser.add_argument("--to-date", required=True)
    run_llm_eval_auto_range_parser.add_argument("--prompt-version", default="v1")
    run_llm_eval_auto_range_parser.add_argument("--window-trading-days", type=int, default=20)
    run_llm_eval_auto_range_parser.add_argument("--output-dir")
    run_llm_eval_auto_range_parser.add_argument("--overwrite-existing", action="store_true")
    run_llm_eval_auto_range_parser.add_argument("--reasoning-effort", choices=["low", "medium", "high", "xhigh"])
    prepare_openai_batch_range_parser = subparsers.add_parser("prepare-openai-batch-range")
    prepare_openai_batch_range_parser.add_argument("--code", required=True)
    prepare_openai_batch_range_parser.add_argument("--model", required=True)
    prepare_openai_batch_range_parser.add_argument("--from-date", required=True)
    prepare_openai_batch_range_parser.add_argument("--to-date", required=True)
    prepare_openai_batch_range_parser.add_argument("--prompt-version", default="v1")
    prepare_openai_batch_range_parser.add_argument("--window-trading-days", type=int, default=20)
    prepare_openai_batch_range_parser.add_argument("--output-dir")
    prepare_openai_batch_range_parser.add_argument("--overwrite-existing", action="store_true")
    prepare_openai_batch_range_parser.add_argument("--reasoning-effort", choices=["low", "medium", "high", "xhigh"])
    submit_openai_batch_parser = subparsers.add_parser("submit-openai-batch")
    submit_openai_batch_parser.add_argument("--batch-dir", required=True)
    submit_openai_batch_parser.add_argument("--completion-window", default="24h")
    finalize_openai_batch_parser = subparsers.add_parser("finalize-openai-batch")
    finalize_openai_batch_parser.add_argument("--batch-dir", required=True)
    finalize_openai_batch_parser.add_argument("--batch-id")
    export_stock_factors = subparsers.add_parser("export-stock-factors")
    export_stock_factors.add_argument("--code", required=True)
    export_stock_factors.add_argument("--date")
    export_stock_factors.add_argument("--output-dir")

    return parser


def _add_jquants_range_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--code")
    parser.add_argument("--date")
    parser.add_argument("--from-date")
    parser.add_argument("--to-date")


if __name__ == "__main__":
    main()
