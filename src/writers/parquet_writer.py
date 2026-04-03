from pyspark.sql import DataFrame

from .base import BaseWriter


class ParquetWriter(BaseWriter):
    def write(self, df: DataFrame, path: str, mode: str, options: dict) -> None:
        df.write.mode(mode).options(**options).parquet(path)
