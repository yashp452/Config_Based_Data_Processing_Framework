from pyspark.sql import DataFrame

from .base import BaseTransformer


class DropDuplicatesTransformer(BaseTransformer):
    """Remove duplicate rows.

    params:
        subset: list of column names to consider (optional — all columns if omitted)
    """

    def transform(self, df: DataFrame) -> DataFrame:
        subset: list | None = self.params.get("subset") or None
        return df.dropDuplicates(subset)
