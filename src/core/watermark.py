"""Watermark manager backed by a Delta table on MinIO.

Tracks per-dataset pipeline state (last run, row count, status) using Delta
MERGE so re-runs are always idempotent and no external DB client is needed.
"""
import logging
import os
from datetime import date, datetime

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

_WATERMARK_PATH = "s3a://{bucket}/control/watermark/".format(
    bucket=os.getenv("MINIO_BUCKET", "retail")
)

_SCHEMA = (
    "dataset_name STRING, layer STRING, last_processed_date DATE, "
    "last_run_timestamp TIMESTAMP, rows_processed LONG, status STRING"
)


class WatermarkManager:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self._path = _WATERMARK_PATH

    def get(self, dataset_name: str) -> dict | None:
        """Return the latest watermark row for a dataset, or None."""
        try:
            from delta.tables import DeltaTable
            if not DeltaTable.isDeltaTable(self.spark, self._path):
                return None
            rows = (
                self.spark.read.format("delta").load(self._path)
                .filter(f"dataset_name = '{dataset_name}'")
                .collect()
            )
            return rows[0].asDict() if rows else None
        except Exception as exc:
            logger.debug("Watermark read skipped (%s)", exc)
            return None

    def write(
        self,
        dataset_name: str,
        layer: str,
        rows: int,
        last_date: date,
        status: str,
    ) -> None:
        """Upsert a watermark row using Delta MERGE."""
        try:
            from delta.tables import DeltaTable

            new_row = self.spark.createDataFrame(
                [{
                    "dataset_name": dataset_name,
                    "layer": layer,
                    "last_processed_date": last_date,
                    "last_run_timestamp": datetime.utcnow(),
                    "rows_processed": rows,
                    "status": status,
                }],
                schema=_SCHEMA,
            )

            if DeltaTable.isDeltaTable(self.spark, self._path):
                DeltaTable.forPath(self.spark, self._path).alias("t").merge(
                    new_row.alias("s"),
                    "t.dataset_name = s.dataset_name",
                ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
            else:
                new_row.write.format("delta").mode("overwrite").save(self._path)
        except Exception as exc:
            logger.error("Watermark write failed for %s: %s", dataset_name, exc)
