from pyspark.sql import DataFrame
from pyspark.sql.functions import current_date

from .base import BaseTransformer


class AddIngestionDateTransformer(BaseTransformer):
    """Adds the current date as a new column (default name: ingestion_date).

    params:
        column: target column name (default "ingestion_date")
    """

    def transform(self, df: DataFrame) -> DataFrame:
        col_name = self.params.get("column", "ingestion_date")
        return df.withColumn(col_name, current_date())
