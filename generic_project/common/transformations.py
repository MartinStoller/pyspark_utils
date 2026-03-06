import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    coalesce,
    col,
    lit,
    regexp_replace,
    to_date,
    to_timestamp,
)
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    StringType,
    StructType,
    TimestampType,
)


def replace_null_like_values(df: DataFrame) -> DataFrame:
    df = df.na.replace("", None)
    df = df.na.replace("-", None)
    # add or remove based on your own data
    return df


def parse_percent_value_column(df: DataFrame, column_name: str) -> DataFrame:
    numeric_str = regexp_replace(col(column_name), "%", "")
    ratio = (numeric_str.cast("double") / lit(100.0)).cast(DecimalType(10, 7))

    return df.withColumn(
        column_name,
        ratio,
    )


def cast_df_columns_by_schema(
    df: DataFrame,
    schema: StructType,
) -> DataFrame:
    """Convert dataframe columns to their types according to schema."""
    for field in schema:
        field_name = field.name

        if field_name not in df.columns:
            continue

        match field.dataType:
            case DateType():
                df = df.withColumn(
                    field_name,
                    coalesce(
                        to_date(col(field_name), "dd/MM/yyyy"),
                        to_date(col(field_name), "dd.MM.yyyy"),
                        to_date(col(field_name), "yyyy-MM-dd"),
                    ),
                )

            case DecimalType():
                df = df.withColumn(
                    field_name,
                    col(field_name).cast(DecimalType(field.dataType.precision, field.dataType.scale)),
                )

            case DoubleType():
                df = df.withColumn(field_name, col(field_name).cast("double"))

            case FloatType():
                df = df.withColumn(field_name, col(field_name).cast("float"))

            case IntegerType():
                df = df.withColumn(field_name, col(field_name).cast("int"))

            case StringType():
                # String is the default, so no need for any conversion
                ...

            case TimestampType():
                df = df.withColumn(
                    field_name,
                    coalesce(
                        to_timestamp(col(field_name), "dd/MM/yyyy HH:mm"),
                        to_timestamp(col(field_name), "dd/MM/yyyy HH:mm:ss"),
                        to_timestamp(col(field_name), "dd.MM.yyyy HH:mm"),
                        to_timestamp(col(field_name), "dd.MM.yyyy HH:mm:ss"),
                        to_timestamp(col(field_name), "yyyy-MM-dd HH:mm"),
                        to_timestamp(col(field_name), "yyyy-MM-dd HH:mm:ss"),
                    ),
                )
            case BooleanType():
                df = df.withColumn(field_name, col(field_name).cast("boolean"))

            case _:
                print("Unsupported data type for column conversion: %s", field.dataType)
    return df


def rename_df_according_to_name_mapping(df: DataFrame, mapping: dict[str, str]) -> DataFrame:
    for csv_col, db_col in mapping.items():
        if csv_col in df.columns:
            df = df.withColumnRenamed(csv_col, db_col)
    return df


def ensure_utc_timestamps_pandas(pandas_df: pd.DataFrame, timestamp_columns: list[str]) -> pd.DataFrame:
    """
    Ensure specified datetime columns in the pandas DataFrame are timezone-aware in UTC.
    Converts naive timestamps to UTC and converts any other timezones to UTC.

    Args:
        pandas_df: Input pandas DataFrame.
        timestamp_columns: List of column names to treat as timestamps.

    Returns:
        pandas DataFrame with specified columns UTC-localized.
    """
    for column in timestamp_columns:
        if column in pandas_df.columns:
            pandas_df[column] = pd.to_datetime(pandas_df[column], errors="coerce")
            if pandas_df[column].dt.tz is None:
                pandas_df[column] = pandas_df[column].dt.tz_localize("UTC")
            else:
                pandas_df[column] = pandas_df[column].dt.tz_convert("UTC")
    return pandas_df


def are_there_empty_lines_sanity_check(df: DataFrame, column: str) -> DataFrame:
    """
    helpful when provided with csv files or other semi structured formats, which have a history of being of poor
    quality. Use a column that is not nullable for detecting and pruning falsy rows.
    """
    empty_rows = df.filter(col(column).isNull()).count()
    if empty_rows > 0:
        print(f"WARNING: {empty_rows} Rows are empty. Removing them from the DataFrame.")
    return df.filter(col(column).isNotNull())
