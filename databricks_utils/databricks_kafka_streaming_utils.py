from typing import Dict

from databricks.sdk.runtime import dbutils
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


def get_param(name: str, default: str) -> str:
    # Databricks Jobs can pass task parameters as widgets.
    try:
        return dbutils.widgets.get(name)
    except Exception:
        return default


def get_secret(scope: str, key: str, default: str = "") -> str:
    try:
        return dbutils.secrets.get(scope=scope, key=key)
    except Exception as e:
        print(e)
        return default


def _confluent_auth_options(
    kafka_api_key: str,
    kafka_api_secret: str,
    kafka_security_protocol: str,
    kafka_sasl_mechanism: str,
) -> Dict[str, str]:
    options = {
        "kafka.security.protocol": kafka_security_protocol,
        "kafka.sasl.mechanism": kafka_sasl_mechanism,
    }

    if kafka_security_protocol.upper().startswith("SASL"):
        if not kafka_api_key or not kafka_api_secret:
            raise ValueError(
                "KAFKA_API_KEY and KAFKA_API_SECRET are required for SASL Kafka connections " "(e.g., Confluent Cloud)."
            )
        options["kafka.sasl.jaas.config"] = (
            "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="{kafka_api_key}" password="{kafka_api_secret}";'
        )

    return options


def build_web_events_stream(
    spark,
    kafka_bootstrap: str,
    kafka_topic: str,
    starting_offsets: str = "latest",
    kafka_api_key: str = "",
    kafka_api_secret: str = "",
    kafka_security_protocol: str = "SASL_SSL",
    kafka_sasl_mechanism: str = "PLAIN",
) -> DataFrame:
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
    )

    auth_options = _confluent_auth_options(
        kafka_api_key=kafka_api_key,
        kafka_api_secret=kafka_api_secret,
        kafka_security_protocol=kafka_security_protocol,
        kafka_sasl_mechanism=kafka_sasl_mechanism,
    )
    for key, value in auth_options.items():
        reader = reader.option(key, value)

    raw_stream = reader.load()
    json_str_col = F.col("value").cast("string")

    return (
        raw_stream.select(
            F.col("key").cast("string").alias("event_key"),
            F.col("timestamp").alias("kafka_timestamp"),
            json_str_col.alias("raw_json"),
        )
        .select(
            "event_key",
            "kafka_timestamp",
            "raw_json",
            F.get_json_object("raw_json", "$.url").alias("url"),
            F.get_json_object("raw_json", "$.referrer").alias("referrer"),
            F.get_json_object("raw_json", "$.user_agent").alias("user_agent"),
            F.from_json(
                F.get_json_object("raw_json", "$.headers"),
                T.MapType(T.StringType(), T.StringType()),
            ).alias("headers"),
            F.get_json_object("raw_json", "$.host").alias("host"),
            F.get_json_object("raw_json", "$.ip").alias("ip"),
            F.get_json_object("raw_json", "$.event_time").alias("event_time_raw"),
        )
        .withColumn(
            "event_time",
            F.coalesce(
                F.to_timestamp("event_time_raw"),
                F.to_timestamp("event_time_raw", "yyyy-MM-dd'T'HH:mm:ss.SSSX"),
                F.to_timestamp("event_time_raw", "yyyy-MM-dd'T'HH:mm:ssX"),
            ),
        )
        .withColumn("ingest_time", F.current_timestamp())
    )
