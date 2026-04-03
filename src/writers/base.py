from abc import ABC, abstractmethod

from pyspark.sql import DataFrame


class BaseWriter(ABC):
    @abstractmethod
    def write(self, df: DataFrame, path: str, mode: str, options: dict) -> None:
        pass
