import os

from delta import DeltaTable
from pyspark.sql import SparkSession

from databricks_utils.spark_session_and_environment import is_databricks


def cleanup_path(spark: SparkSession, path: str) -> None:
    """
    Remove a Delta table path if it exists.
    Works in both local and Databricks environments.
    """
    if is_databricks():
        # Use dbutils in Databricks
        try:
            from pyspark.dbutils import DBUtils

            dbutils = DBUtils(spark)
            dbutils.fs.rm(path, recurse=True)
        except Exception:
            # If dbutils not available, try SQL
            try:
                spark.sql(f"DROP TABLE IF EXISTS delta.`{path}`")
            except Exception:
                pass
    else:
        import shutil

        if os.path.exists(path):
            shutil.rmtree(path)


def view_delta_table_history(spark: SparkSession, table_path: str) -> None:
    delta_table = DeltaTable.forPath(spark, table_path)
    history = delta_table.history()

    print("\nAll versions with operations:")
    history.select("version", "timestamp", "operation", "operationMetrics").orderBy("version").show(truncate=False)


def get_table_file_stats(spark: SparkSession, table_path: str) -> dict:
    """Get statistics about the files in a Delta table."""
    if is_databricks():
        detail = spark.sql(f"DESCRIBE DETAIL delta.`{table_path}`").collect()[0]
        return {
            "num_files": detail["numFiles"],
            "total_size_mb": detail["sizeInBytes"] / (1024 * 1024),
            "avg_file_size_mb": (detail["sizeInBytes"] / detail["numFiles"]) / (1024 * 1024)
            if detail["numFiles"] > 0
            else 0,
            "files": [],
        }
    else:
        parquet_files = []
        for root, dirs, files in os.walk(table_path):
            for f in files:
                if f.endswith(".parquet"):
                    file_path = os.path.join(root, f)
                    parquet_files.append({"name": f, "size_mb": os.path.getsize(file_path) / (1024 * 1024)})

        return {
            "num_files": len(parquet_files),
            "total_size_mb": sum(f["size_mb"] for f in parquet_files),
            "avg_file_size_mb": sum(f["size_mb"] for f in parquet_files) / len(parquet_files) if parquet_files else 0,
            "files": parquet_files,
        }


def count_parquet_files(spark: SparkSession, table_path: str) -> tuple[int, float]:
    """Count parquet files in the table directory and their size."""
    if is_databricks():
        detail = spark.sql(f"DESCRIBE DETAIL delta.`{table_path}`").collect()[0]
        return detail["numFiles"], detail["sizeInBytes"] / (1024 * 1024)
    else:
        import os

        count = 0
        total_size = 0
        for root, dirs, files in os.walk(table_path):
            if "_delta_log" in root:
                continue
            for f in files:
                if f.endswith(".parquet"):
                    count += 1
                    total_size += os.path.getsize(os.path.join(root, f))
        return count, total_size / (1024 * 1024)


def add_clustering_to_existing_table(spark: SparkSession, table_name: str, clustering_columns: list) -> None:
    """
    Add liquid clustering to an existing Delta table.

    Note: This only sets the clustering columns for future writes.
    Existing data needs to be re-clustered using OPTIMIZE.
    """
    columns_str = ", ".join(clustering_columns)

    spark.sql(f"""
        ALTER TABLE {table_name}
        CLUSTER BY ({columns_str})
    """)

    print(f"Added clustering to {table_name}")
    print(f"Clustering columns: {columns_str}")
    print("Run OPTIMIZE to cluster existing data")


def remove_clustering(spark: SparkSession, table_name: str) -> None:
    """
    Remove liquid clustering from a table.

    This removes the clustering configuration but doesn't change
    the physical data layout.
    """
    spark.sql(f"""
        ALTER TABLE {table_name}
        CLUSTER BY NONE
    """)

    print(f"Removed clustering from {table_name}")
