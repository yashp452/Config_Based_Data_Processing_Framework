from pyspark.sql import SparkSession

from .base import BaseReader
from .csv_reader import CsvReader
from .delta_reader import DeltaReader
from .parquet_reader import ParquetReader

_REGISTRY: dict[str, type[BaseReader]] = {
    "csv": CsvReader,
    "parquet": ParquetReader,
    "delta": DeltaReader,
}


class ReaderFactory:
    @staticmethod
    def get(format: str, spark: SparkSession) -> BaseReader:
        if format not in _REGISTRY:
            raise ValueError(
                f"Unsupported reader format: '{format}'. "
                f"Supported: {sorted(_REGISTRY)}"
            )
        return _REGISTRY[format](spark)

    @staticmethod
    def register(format: str, cls: type[BaseReader]) -> None:
        _REGISTRY[format] = cls
