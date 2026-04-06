import os

from pyspark.sql import SparkSession

from ..core.config_loader import validate
from ..core.pipeline import Pipeline


class Container:
    """DI container — wires SparkSession (with Delta + MinIO) and pipeline."""

    def __init__(self, app_name: str = "DataPipeline"):
        self._app_name = app_name
        self._spark: SparkSession | None = None

    @property
    def spark(self) -> SparkSession:
        if self._spark is None:
            self._spark = self._build_session()
        return self._spark

    def _build_session(self) -> SparkSession:
        endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
        access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")

        return (
            SparkSession.builder
            .appName(self._app_name)
            .master("local[*]")
            # Delta Lake
            .config("spark.sql.extensions",
                    "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.delta.logStore.s3a.impl",
                    "io.delta.storage.S3SingleDriverLogStore")
            # Dynamic partition overwrite (Bronze incremental)
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            # MinIO / S3A
            .config("spark.hadoop.fs.s3a.endpoint", endpoint)
            .config("spark.hadoop.fs.s3a.access.key", access_key)
            .config("spark.hadoop.fs.s3a.secret.key", secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl",
                    "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            # Performance
            .config("spark.driver.memory", "4g")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )

    def pipeline_from_config(self, config: dict) -> Pipeline:
        validate(config)
        return Pipeline(self.spark, config)

    def teardown(self) -> None:
        if self._spark:
            self._spark.stop()
            self._spark = None
