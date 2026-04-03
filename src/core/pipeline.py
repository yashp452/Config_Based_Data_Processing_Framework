import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import expr

from ..readers.factory import ReaderFactory
from ..transformers.factory import TransformerFactory
from ..writers.factory import WriterFactory

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config

    def run(self) -> int:
        """Execute read → join → transform → enforce_schema → aggregate → write. Returns row count."""
        df = self._read()
        df = self._join(df)
        df = self._transform(df)
        df = self._enforce_schema(df)
        df = self._aggregate(df)
        df = df.persist()
        row_count = df.count()
        logger.info("Writing %d rows", row_count)
        self._write(df)
        df.unpersist()
        return row_count

    # ------------------------------------------------------------------

    def _read(self) -> DataFrame:
        source = self.config["source"]
        reader = ReaderFactory.get(source["format"], self.spark)
        logger.info("Reading from %s (format=%s)", source["path"], source["format"])
        return reader.read(source["path"], source.get("options", {}))

    def _join(self, df: DataFrame) -> DataFrame:
        for j in self.config.get("joins", []):
            right = ReaderFactory.get(j["format"], self.spark).read(
                j["path"], j.get("options", {})
            )
            # Optionally restrict which columns come from the right side
            select_cols = j.get("select", [])
            on_col = j["on"]
            if select_cols:
                if on_col not in select_cols:
                    select_cols = [on_col] + select_cols
                right = right.select(*select_cols)
            logger.info("Joining %s on '%s' (%s)", j["path"], on_col, j.get("type", "left"))
            df = df.join(right, on_col, j.get("type", "left"))
        return df

    def _transform(self, df: DataFrame) -> DataFrame:
        for step in self.config.get("transformations", []):
            name, params = step["name"], step.get("params", {})
            logger.info("Applying transformer: %s", name)
            try:
                df = TransformerFactory.get(name, params).transform(df)
            except Exception as exc:
                raise RuntimeError(f"Transformer '{name}' failed: {exc}") from exc
        return df

    def _enforce_schema(self, df: DataFrame) -> DataFrame:
        schema_path = self.config.get("schema_path")
        if not schema_path:
            return df
        from .schema_enforcer import enforce_schema
        on_null_violation = self.config.get("on_null_violation", "warn")
        logger.info("Enforcing schema from %s", schema_path)
        return enforce_schema(df, schema_path, on_null_violation)

    def _aggregate(self, df: DataFrame) -> DataFrame:
        agg_cfg = self.config.get("aggregation")
        if not agg_cfg:
            return df
        group_cols = agg_cfg["group_by"]
        metrics = [expr(m["expr"]).alias(m["name"]) for m in agg_cfg["metrics"]]
        logger.info("Aggregating by %s", group_cols)
        return df.groupBy(*group_cols).agg(*metrics)

    def _write(self, df: DataFrame) -> None:
        sink = self.config["sink"]
        fmt = sink["format"]
        mode = sink.get("mode", "overwrite")
        path = sink.get("path", "")
        # For postgres, pass sink options (table name etc.) via options dict
        options = {k: v for k, v in sink.items() if k not in ("format", "path", "mode")}
        writer = WriterFactory.get(fmt)
        logger.info("Writing to '%s' (format=%s, mode=%s)", path or sink.get("table"), fmt, mode)
        writer.write(df, path, mode, options)
