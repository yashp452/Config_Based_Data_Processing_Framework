from pyspark.sql import DataFrame

from .base import BaseReader


class DeltaReader(BaseReader):
    """Reads a Delta table from any path (local or s3a://)."""

    def read(self, path: str, options: dict) -> DataFrame:
        return self.spark.read.format("delta").options(**options).load(path)
