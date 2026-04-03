import logging

from pyspark.sql import DataFrame

from .base import BaseWriter

logger = logging.getLogger(__name__)


class DeltaWriter(BaseWriter):
    """Writes a Delta table to any path (local or s3a://).

    Supported modes (via sink config):
      overwrite         — full overwrite (default)
      overwrite+dynamic — dynamic partition overwrite (Bronze incremental)
      merge             — Delta MERGE on a merge_key (Silver upserts)
    """

    def write(self, df: DataFrame, path: str, mode: str, options: dict) -> None:
        if mode == "merge":
            self._merge(df, path, options)
        else:
            writer = df.write.format("delta").mode("overwrite")
            partition_by = options.get("partition_by", [])
            if partition_by:
                writer = writer.partitionBy(*partition_by)
            if options.get("overwrite_mode") == "dynamic":
                writer = writer.option("partitionOverwriteMode", "dynamic")
            logger.info("Delta write → %s (partitionBy=%s)", path, partition_by)
            writer.save(path)

    def _merge(self, df: DataFrame, path: str, options: dict) -> None:
        from delta.tables import DeltaTable

        merge_key = options["merge_key"]
        spark = df.sparkSession

        if DeltaTable.isDeltaTable(spark, path):
            logger.info("Delta merge on %s (key=%s)", path, merge_key)
            spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
            DeltaTable.forPath(spark, path).alias("t").merge(
                df.alias("s"),
                f"t.{merge_key} = s.{merge_key}",
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        else:
            logger.info("Delta table not found at %s — full write", path)
            df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(path)
