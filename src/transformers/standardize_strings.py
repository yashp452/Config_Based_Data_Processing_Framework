from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lower, trim

from .base import BaseTransformer


class StandardizeStringsTransformer(BaseTransformer):
    """Trim whitespace and optionally lowercase string columns.

    params:
        columns:   list of column names to standardize
        lowercase: bool — apply lower() in addition to trim() (default True)
    """

    def transform(self, df: DataFrame) -> DataFrame:
        columns: list = self.params.get("columns", [])
        apply_lower: bool = self.params.get("lowercase", True)
        for c in columns:
            expr = trim(col(c))
            if apply_lower:
                expr = lower(expr)
            df = df.withColumn(c, expr)
        return df
