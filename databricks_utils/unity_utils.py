from spark.sql import SparkSession


def show_owner(spark: SparkSession, object_type: str, object_name: str) -> None:
    """
    Show the current owner of an object.
    """
    if object_type.upper() == "CATALOG":
        result = spark.sql(f"DESCRIBE CATALOG EXTENDED {object_name}").collect()
    elif object_type.upper() == "SCHEMA":
        result = spark.sql(f"DESCRIBE SCHEMA EXTENDED {object_name}").collect()
    else:
        result = spark.sql(f"DESCRIBE EXTENDED {object_name}").collect()

    for row in result:
        if hasattr(row, "info_name") and "Owner" in str(row.info_name):
            print(f"Owner of {object_type} {object_name}: {row.info_value}")
            return row.info_value
    print(f"Could not determine owner for {object_type} {object_name}")
