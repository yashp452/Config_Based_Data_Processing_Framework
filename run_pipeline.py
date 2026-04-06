"""Medallion pipeline orchestrator.

Usage:
    python run_pipeline.py --layer all
    python run_pipeline.py --layer bronze
    python run_pipeline.py --layer silver --full-refresh
    python run_pipeline.py --layer gold
"""
import argparse
import glob
import json
import logging
import sys
from datetime import date, datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

JOB_GLOB  = "configs/jobs/job_*.json"   # bronze + silver (one file per dataset)
GOLD_GLOB = "configs/gold/*.json"        # gold aggregations (separate by design)


# ── Config helpers ────────────────────────────────────────────────────────────

def _load_json(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


def _resolve_date_template(path: str, run_date: date) -> str:
    """Replace {year}/{month}/{day} placeholders for facts folder paths."""
    return path.format(
        year=run_date.year,
        month=f"{run_date.month:02d}",
        day=f"{run_date.day:02d}",
    )


def _to_bronze_config(job: dict, run_date: date) -> dict:
    b = job["bronze"]
    source_path = _resolve_date_template(b["source_path"], run_date)
    return {
        "dataset": f"bronze_{job['dataset']}",
        "layer": "bronze",
        "source": {"format": b["source_format"], "path": source_path, "options": {}},
        "transformations": [
            {"name": "add_ingestion_date", "params": {"column": "ingestion_date"}}
        ],
        "sink": {
            "format": "delta",
            "path": b["target_path"],
            "mode": "overwrite",
            "partition_by": ["ingestion_date"],
            "overwrite_mode": "dynamic",
        },
    }


def _to_silver_config(job: dict, incremental_from: str | None = None) -> dict:
    sv = job["silver"]
    # DQ transformations come first, then any dataset-specific ones
    transformations = [
        {"name": "filter_nulls",    "params": {"columns": sv["not_null_columns"]}},
        {"name": "drop_duplicates", "params": {"subset": sv["dedup_key"]}},
    ] + sv.get("transformations", [])

    # Pass incremental filter as a source option — pipeline applies it after read
    source_options = {}
    if incremental_from:
        source_options["incremental_from"] = incremental_from

    return {
        "dataset": f"silver_{job['dataset']}",
        "layer": "silver",
        "source": {"format": "delta", "path": sv["source_path"], "options": source_options},
        "joins": sv.get("joins", []),
        "transformations": transformations,
        "schema_path": sv.get("schema_path"),
        "on_null_violation": sv.get("on_null_violation", "warn"),
        "sink": {
            "format": "delta",
            "path": sv["target_path"],
            "mode": "merge",
            "merge_key": sv["primary_key"],
        },
    }


def _load_pipeline_configs(layer: str, run_date: date, watermark=None, dataset: str | None = None) -> list[dict]:
    if layer == "gold":
        configs = [_load_json(p) for p in sorted(glob.glob(GOLD_GLOB))]
        if dataset:
            configs = [c for c in configs if c["dataset"] == dataset]
        return configs

    job_paths = sorted(glob.glob(JOB_GLOB))
    if not job_paths:
        return []
    jobs = [_load_json(p) for p in job_paths]
    if dataset:
        jobs = [j for j in jobs if j["dataset"] == dataset]
    if layer == "bronze":
        return [_to_bronze_config(j, run_date) for j in jobs]
    else:  # silver
        configs = []
        for j in jobs:
            incremental_from = None
            if watermark:
                silver_dataset = f"silver_{j['dataset']}"
                wm = watermark.get(silver_dataset)
                if wm and wm.get("status") == "success":
                    incremental_from = str(wm["last_processed_date"])
                    logger.info(
                        "[SILVER] %s — incremental read from ingestion_date > %s",
                        silver_dataset, incremental_from,
                    )
            configs.append(_to_silver_config(j, incremental_from))
        return configs


# ── Runner ────────────────────────────────────────────────────────────────────

def run_layer(container, watermark, layer: str, full_refresh: bool, run_date: date, dataset: str | None = None) -> list[str]:
    """Run all datasets for one layer. Returns names of failed datasets."""
    # Pass watermark to silver so each dataset only reads new bronze partitions
    wm_for_silver = None if full_refresh else watermark
    configs = _load_pipeline_configs(layer, run_date, wm_for_silver, dataset)
    if not configs:
        logger.warning("No configs found for layer '%s'", layer)
        return []

    failures: list[str] = []
    for config in configs:
        dataset = config["dataset"]

        # Skip datasets already successfully processed today (unless --full-refresh)
        if not full_refresh:
            wm = watermark.get(dataset)
            if wm:
                last_date = str(wm.get("last_processed_date", ""))
                status    = wm.get("status", "")
                if last_date == str(run_date) and status == "success":
                    logger.info(
                        "[%s] ↷ SKIPPED %s — already processed on %s",
                        layer.upper(), dataset, run_date,
                    )
                    continue
                logger.info(
                    "[%s] last run: %s | %s rows | status=%s",
                    layer.upper(), last_date, wm.get("rows_processed"), status,
                )

        logger.info("[%s] ▶  %s", layer.upper(), dataset)
        start = datetime.utcnow()

        try:
            pipeline = container.pipeline_from_config(config)
            rows     = pipeline.run()
            elapsed  = (datetime.utcnow() - start).total_seconds()
            logger.info("[%s] ✓  %s — %d rows in %.1fs", layer.upper(), dataset, rows, elapsed)
            watermark.write(dataset, layer, rows, run_date, "success")
        except Exception as exc:
            # Gracefully skip facts files that don't exist for the given date
            exc_str = str(exc)
            if "Path does not exist" in exc_str or "No such file" in exc_str or "FileNotFoundError" in exc_str:
                elapsed = (datetime.utcnow() - start).total_seconds()
                logger.warning("[%s] ↷  %s — source file not found for %s, skipping", layer.upper(), dataset, run_date)
                watermark.write(dataset, layer, 0, run_date, "skipped")
            else:
                elapsed = (datetime.utcnow() - start).total_seconds()
                logger.error("[%s] ✗  %s — FAILED (%.1fs): %s", layer.upper(), dataset, elapsed, exc)
                watermark.write(dataset, layer, 0, run_date, "failed")
                failures.append(dataset)

    return failures


def main() -> None:
    parser = argparse.ArgumentParser(description="Medallion pipeline orchestrator")
    parser.add_argument(
        "--layer", choices=["bronze", "silver", "gold", "all"], required=True,
    )
    parser.add_argument(
        "--full-refresh", action="store_true",
        help="Ignore watermark and reprocess everything from scratch",
    )
    parser.add_argument(
        "--date", default=None,
        help="Run date as YYYY-MM-DD (default: today). Determines which facts folder to read.",
    )
    parser.add_argument(
        "--dataset", default=None,
        help="Process a single dataset only (e.g. retail_orders, gold_daily_trend).",
    )
    args = parser.parse_args()

    run_date = date.fromisoformat(args.date) if args.date else date.today()
    logger.info("Run date: %s", run_date)

    from src.di.container import Container
    from src.core.watermark import WatermarkManager

    container = Container()
    watermark = WatermarkManager(container.spark)

    layers = ["bronze", "silver", "gold"] if args.layer == "all" else [args.layer]

    for layer in layers:
        failures = run_layer(container, watermark, layer, args.full_refresh, run_date, args.dataset)
        if failures and layer in ("bronze", "silver"):
            logger.error(
                "Layer '%s' had %d failure(s) — stopping. Failed: %s",
                layer, len(failures), failures,
            )
            container.teardown()
            sys.exit(1)

    container.teardown()
    logger.info("Pipeline run complete.")


if __name__ == "__main__":
    main()
