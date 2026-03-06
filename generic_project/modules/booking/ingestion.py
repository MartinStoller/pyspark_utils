"""
Dummy of what an ingestion job looks like structurally.
"""

from common.db import read_csv_using_schema, write_df_to_db
from common.etl_base import ETLJob
from common.logging_utils import get_df_preview, get_logger
from common.transformations import are_there_empty_lines_sanity_check
from pyspark.sql import DataFrame, SparkSession
from shared.schemas.booking_schemas import BookingBronze, BookingCSV


class BookingIngestion(ETLJob):
    def __init__(
        self,
        booking_path: str,
        glue_context=None,
        spark: SparkSession = None,
    ):
        self.logger = get_logger("booking_core_logger")
        self.booking_path = booking_path
        self.glue_context = glue_context
        self.spark = spark

    def extract(self) -> DataFrame:
        self.logger.info(f"Extracting booking CSV from {self.booking_path}")
        new_csv_data = read_csv_using_schema(
            csv_path=self.booking_path,
            schema=BookingCSV.schema,
            spark=self.spark,
        )

        self.logger.info(
            f"Raw booking CSV extracted: {new_csv_data.count()} rows, " f"{len(new_csv_data.columns)} columns"
        )
        self.logger.info("\n" + get_df_preview(new_csv_data, n=5))
        return new_csv_data

    def transform(self, *inputs: DataFrame) -> DataFrame:
        (new_csv_data,) = inputs
        self.logger.info("Transforming booking CSV → booking_bronze")

        result = are_there_empty_lines_sanity_check(new_csv_data, column=BookingCSV.TRANSACTION_ID, logger=self.logger)

        # more transformations...

        self.logger.info("Transformation finished:")
        self.logger.info("\n" + get_df_preview(result, n=5))
        return result

    def load(self, *outputs: DataFrame) -> None:
        (bookings,) = outputs
        self.logger.info(f"Loading results into table {BookingBronze.TABLE}")

        write_df_to_db(
            result_df=bookings,
            target_table=BookingBronze.TABLE,
            schema=BookingBronze.schema,
        )

        self.logger.info(f"Inserted {bookings.count()} rows into {BookingBronze.TABLE}")

    def run(self) -> None:
        self.logger.info("Starting booking Ingestion...")

        new_csv_data = self.extract()
        booking_core = self.transform(new_csv_data)
        self.load(booking_core)

        self.logger.info("Booking Ingestion finished successfully.")
