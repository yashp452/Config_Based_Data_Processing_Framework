from pyspark.sql import DataFrame

from .base import BaseTransformer


class FilterNullsTransformer(BaseTransformer):
    """Drop rows that contain null values.

    params:
        columns: list of column names to check (optional — all columns if omitted)
    """

    def transform(self, df: DataFrame) -> DataFrame:
        columns: list = self.params.get("columns") or []
        return df.dropna(subset=columns if columns else None)
