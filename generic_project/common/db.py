"""
Generic Utility functions concerning I/O stuff. --> Reading/Writing from/to different sources.
"""

import re
import time

import psycopg2
import pyspark
from common.logging_utils import get_df_preview
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    lit,
)
from shared.config import (
    BATCH_SIZE,
    DB_DRIVER,
    DB_HOST,
    DB_NAME,
    DB_PASSWORD,
    DB_PORT,
    DB_URL,
    DB_USER,
    MAX_RETRIES,
    RETRY_WAIT_SECONDS,
)


def get_details_from_jdbc_url(jdbc_url: str) -> tuple[str, str]:
    pattern = r"jdbc:[a-zA-Z0-9]+://([^:/]+)(?::(\d+))?"
    match = re.search(pattern, jdbc_url)

    if not match:
        raise ValueError(f"Invalid JDBC URL format: {jdbc_url}")

    host = match.group(1)
    port = match.group(2) or ""
    return host, port


def load_table_from_db(spark: SparkSession, table_name: str) -> DataFrame:
    return (
        spark.read.format("jdbc")
        .option("url", DB_URL)
        .option("driver", DB_DRIVER)
        .option("dbtable", table_name)
        .option("user", DB_USER)
        .option("password", DB_PASSWORD)
        .load()
    )


def execute_postgres_query(sql: str, autocommit: bool = True, fetch_one: bool = False, fetch_all: bool = False):
    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)
    conn.autocommit = autocommit
    result = None

    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            if fetch_one:
                result = cur.fetchone()
            elif fetch_all:
                result = cur.fetchall()
            if not autocommit:
                conn.commit()
    finally:
        conn.close()

    return result


def quote_ident(col: str) -> str:
    """Return a SQL-safe quoted identifier."""
    return f'"{col}"'


def read_csv_using_schema(csv_path: str, schema: pyspark.sql.types.StructType, spark: SparkSession) -> DataFrame:
    print(f"Reading CSV: {csv_path}")
    df_raw = spark.read.option("header", True).csv(csv_path)

    schema_columns = [field.name for field in schema.fields]

    available_columns = [c for c in schema_columns if c in df_raw.columns]
    df = df_raw.select(*available_columns)

    for field in schema.fields:
        if field.name not in df.columns:
            df = df.withColumn(field.name, lit(None).cast(field.dataType))

    for field in schema.fields:
        if field.name in df.columns:
            df = df.withColumn(field.name, df[field.name].cast(field.dataType))

    df = df.select(*schema_columns)

    print(f"Successfully read CSV with {len(df.columns)} Columns and {df.count()} rows.:")
    print(get_df_preview(df))
    return df


def replace_all_table_data_jdbc(df: DataFrame, table_name: str) -> None:
    write_options = {
        "url": DB_URL,
        "dbtable": table_name,
        "user": DB_USER,
        "password": DB_PASSWORD,
        "driver": DB_DRIVER,
        "truncate": "true",
        "batchsize": str(BATCH_SIZE),
    }

    for attempt in range(MAX_RETRIES):
        try:
            print(f"Attempt {attempt + 1}/{MAX_RETRIES}: Starting data overwrite for table {table_name}...")

            (df.write.format("jdbc").options(**write_options).mode("overwrite").save())

            print(f"Successfully replaced data in table: {table_name}")
            return

        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                print(f"Write failed on attempt {attempt + 1}. Retrying in {RETRY_WAIT_SECONDS} seconds...")
                print(f"Failure details: {e}")
                time.sleep(RETRY_WAIT_SECONDS)
            else:
                print(f"Final attempt failed to replace data in table {table_name}. All retries exhausted.")
                print(f"Final failure details: {e}")
                raise


def truncate_postgres_table_cascade(table_name: str) -> None:
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )
    conn.autocommit = True

    try:
        with conn.cursor() as cur:
            print(f"Truncating table {table_name} CASCADE...")
            cur.execute(f"TRUNCATE TABLE {table_name} CASCADE;")
            print(f"Successfully truncated table {table_name}")
    finally:
        conn.close()


def overwrite_all_postgres_data_with_cascade(df: DataFrame, table_name: str) -> None:
    write_options = {
        "url": DB_URL,
        "dbtable": table_name,
        "user": DB_USER,
        "password": DB_PASSWORD,
        "driver": DB_DRIVER,
        "batchsize": str(BATCH_SIZE),
    }

    truncate_postgres_table_cascade(table_name)

    for attempt in range(MAX_RETRIES):
        try:
            print(f"Attempt {attempt + 1}/{MAX_RETRIES}: Writing data to {table_name}...")

            (df.write.format("jdbc").options(**write_options).mode("append").save())

            print(f"Successfully replaced data in table: {table_name}")
            return

        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                print(f"Write failed on attempt {attempt + 1}. " f"Retrying in {RETRY_WAIT_SECONDS} seconds...")
                print(f"Failure details: {e}")
                time.sleep(RETRY_WAIT_SECONDS)
            else:
                print(f"Final attempt failed to replace data in table {table_name}. " f"All retries exhausted.")
                print(f"Final failure details: {e}")
                raise


def append_to_table_jdbc(
    df: DataFrame,
    table_name: str,
    mode: str = "append",
    num_partitions: int = 8,
) -> None:
    df = df.coalesce(num_partitions)
    print(f"Writing table {table_name} to DB with {num_partitions} partitions")
    (
        df.write.format("jdbc")
        .option("url", DB_URL)
        .option("driver", DB_DRIVER)
        .option("user", DB_USER)
        .option("password", DB_PASSWORD)
        .option("dbtable", table_name)
        .option("connectTimeout", "120")
        .option("socketTimeout", "300")
        .option("batchsize", BATCH_SIZE)
        .option("numPartitions", num_partitions)
        .option("reconnect", "true")
        .option("tcpKeepAlive", "true")
        .mode(mode)
        .save()
    )
