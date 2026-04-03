"""Silver schema enforcement.

Loads a JSON schema file and applies it to a DataFrame:
  1. Checks all defined columns exist (warns and skips missing ones)
  2. Casts each column to the declared datatype
  3. Checks nullable=false columns — logs a warning or raises based on on_null_violation
  4. Returns only the columns defined in the schema (in declaration order)
"""
import json
import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

logger = logging.getLogger(__name__)


def enforce_schema(df: DataFrame, schema_path: str, on_null_violation: str = "warn") -> DataFrame:
    with open(schema_path) as f:
        schema = json.load(f)

    df_cols = set(df.columns)
    missing = [s["column_name"] for s in schema if s["column_name"] not in df_cols]
    if missing:
        logger.warning("Schema columns not found in dataframe (will be skipped): %s", missing)

    for field in schema:
        name = field["column_name"]
        if name not in df_cols:
            continue

        df = df.withColumn(name, col(name).cast(field["datatype"]))

        if not field["nullable"]:
            null_count = df.filter(col(name).isNull()).count()
            if null_count > 0:
                msg = "DQ: column '%s' has %d null(s) but nullable=false"
                if on_null_violation == "fail":
                    raise ValueError(msg % (name, null_count))
                logger.warning(msg, name, null_count)

    # Return only schema-defined columns that exist, in declaration order
    final_cols = [s["column_name"] for s in schema if s["column_name"] in df_cols]
    return df.select(*final_cols)
