from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from .base import BaseTransformer


class CastTypesTransformer(BaseTransformer):
    """Cast one or more columns to specified Spark SQL types.

    params:
        columns: {"col_name": "spark_type", ...}
    """

    def transform(self, df: DataFrame) -> DataFrame:
        columns: dict = self.params.get("columns", {})
        if not columns:
            raise ValueError("cast_types requires at least one entry in 'columns'")
        for column_name, data_type in columns.items():
            df = df.withColumn(column_name, col(column_name).cast(data_type))
        return df
