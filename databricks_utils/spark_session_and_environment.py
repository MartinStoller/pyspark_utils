import os

from pyspark.sql import SparkSession


def is_databricks() -> bool:
    """Check if running in a Databricks environment."""
    # Check for Databricks-specific environment variables
    return "DATABRICKS_RUNTIME_VERSION" in os.environ or "DB_HOME" in os.environ or os.path.exists("/databricks_utils")


def get_spark() -> SparkSession:
    """
    Get or create a SparkSession configured for Delta Lake.

    In Databricks: Returns the pre-configured spark session
    Locally: Creates a new SparkSession with Delta Lake support

    Returns:
        SparkSession configured for Delta Lake
    """

    if is_databricks():
        return SparkSession.builder.getOrCreate()
    else:
        from delta import configure_spark_with_delta_pip

        builder = (
            SparkSession.builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.databricks_utils.delta.retentionDurationCheck.enabled", "false")
        )

        builder = builder.master("local[*]")

        return configure_spark_with_delta_pip(builder).getOrCreate()


def stop_spark_if_local(spark) -> None:
    if not is_databricks():
        spark.stop()
