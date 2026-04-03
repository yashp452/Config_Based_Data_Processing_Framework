from .base import BaseWriter
from .csv_writer import CsvWriter
from .delta_writer import DeltaWriter
from .parquet_writer import ParquetWriter
from .postgres_writer import PostgresWriter

_REGISTRY: dict[str, type[BaseWriter]] = {
    "csv": CsvWriter,
    "parquet": ParquetWriter,
    "delta": DeltaWriter,
    "postgres": PostgresWriter,
}


class WriterFactory:
    @staticmethod
    def get(format: str) -> BaseWriter:
        if format not in _REGISTRY:
            raise ValueError(
                f"Unsupported writer format: '{format}'. "
                f"Supported: {sorted(_REGISTRY)}"
            )
        return _REGISTRY[format]()

    @staticmethod
    def register(format: str, cls: type[BaseWriter]) -> None:
        _REGISTRY[format] = cls
