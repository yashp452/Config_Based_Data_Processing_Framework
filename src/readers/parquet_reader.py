from pyspark.sql import DataFrame

from .base import BaseReader


class ParquetReader(BaseReader):
    def read(self, path: str, options: dict) -> DataFrame:
        return self.spark.read.options(**options).parquet(path)
