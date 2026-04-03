from abc import ABC, abstractmethod

from pyspark.sql import DataFrame


class BaseTransformer(ABC):
    def __init__(self, params: dict):
        self.params = params

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        pass
