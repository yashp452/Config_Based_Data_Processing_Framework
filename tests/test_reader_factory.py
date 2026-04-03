import pytest

from src.readers.csv_reader import CsvReader
from src.readers.factory import ReaderFactory
from src.readers.parquet_reader import ParquetReader


def test_get_csv_reader(spark):
    reader = ReaderFactory.get("csv", spark)
    assert isinstance(reader, CsvReader)


def test_get_parquet_reader(spark):
    reader = ReaderFactory.get("parquet", spark)
    assert isinstance(reader, ParquetReader)


def test_reader_holds_spark_session(spark):
    reader = ReaderFactory.get("csv", spark)
    assert reader.spark is spark


def test_unsupported_format_raises(spark):
    with pytest.raises(ValueError, match="Unsupported reader format"):
        ReaderFactory.get("excel", spark)


def test_dynamic_registration(spark):
    """Register a custom reader at runtime — no code changes needed elsewhere."""
    from src.readers.base import BaseReader
    from pyspark.sql import DataFrame

    class JsonReader(BaseReader):
        def read(self, path: str, options: dict) -> DataFrame:
            return self.spark.read.options(**options).json(path)

    ReaderFactory.register("json", JsonReader)
    reader = ReaderFactory.get("json", spark)
    assert isinstance(reader, JsonReader)
