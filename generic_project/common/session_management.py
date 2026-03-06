try:
    from awsglue.context import GlueContext
    from pyspark.context import SparkContext

    glue_available = True
except ImportError:
    glue_available = False

from pyspark.sql import SparkSession


def get_default_local_spark_or_glue_session(
    app_name: str, checkpoint_dir: str = None
) -> tuple["GlueContext | None", SparkSession]:
    if glue_available:
        sc = SparkContext.getOrCreate()
        glue_context = GlueContext(sc)
        spark = glue_context.spark_session
        spark.conf.set("spark.sql.session.timeZone", "UTC")
    else:
        spark = (
            SparkSession.builder.appName(app_name)
            .master("local[*]")
            .config("spark.driver.memory", "12g")
            .config("spark.jars.packages", "org.postgresql:postgresql:42.2.27")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.driver.host", "127.0.0.1")
            .getOrCreate()
        )
        glue_context = None

    if checkpoint_dir:
        spark.sparkContext.setCheckpointDir(checkpoint_dir)
    return glue_context, spark
