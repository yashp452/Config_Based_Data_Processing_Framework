import logging
import os

from pyspark.sql import DataFrame

from .base import BaseWriter

logger = logging.getLogger(__name__)


class PostgresWriter(BaseWriter):
    """Writes to PostgreSQL via Spark JDBC.

    Credentials are read from environment variables so they never appear in
    config files:
      POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
    """

    def write(self, df: DataFrame, path: str, mode: str, options: dict) -> None:
        host = os.getenv("POSTGRES_HOST", "postgres")
        port = os.getenv("POSTGRES_PORT", "5432")
        db = os.getenv("POSTGRES_DB", "gold")
        user = os.getenv("POSTGRES_USER", "postgres")
        password = os.getenv("POSTGRES_PASSWORD", "postgres")
        schema = options.get("schema", "public")
        table  = options["table"]
        full_table = f"{schema}.{table}" if schema != "public" else table

        url = f"jdbc:postgresql://{host}:{port}/{db}"
        props = {
            "driver": "org.postgresql.Driver",
            "user": user,
            "password": password,
        }
        logger.info("JDBC write → %s.%s (mode=%s)", db, full_table, mode)
        df.write.jdbc(url=url, table=full_table, mode=mode, properties=props)
