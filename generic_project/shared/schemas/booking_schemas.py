"""
Each Table has its own class with a TABLE property storing the table name, followed by all columns and schema.
Additionally, you may put the mapping between 2 tables in here.
"""

from pyspark.sql.types import DateType, DecimalType, IntegerType, StringType, StructField, StructType


class BookingCSV:
    TRANSACTION_ID = "TRANSACTIONID"
    CREATE_DATE = "CREATEDATE"
    CANCELLATION_DATE = "CANCELLATIONDATE"
    STATUS_CODE = "STATUSCODE"
    schema = StructType(
        [
            StructField(TRANSACTION_ID, StringType(), nullable=True),
            StructField(CREATE_DATE, DateType(), nullable=True),
            StructField(CANCELLATION_DATE, DateType(), nullable=True),
            StructField(STATUS_CODE, IntegerType(), nullable=True),
        ]
    )


class BookingBronze:
    TABLE = "booking_bronze"

    TRANSACTION_ID = "transaction_id"
    GRADE_ID = "grade_id"
    CREATE_DATE = "create_date"
    STATUS_CODE = "status_code"
    DATA_SOURCE = "data_source"

    schema = StructType(
        [
            StructField(TRANSACTION_ID, StringType(), nullable=True),
            StructField(GRADE_ID, StringType(), nullable=True),
            StructField(CREATE_DATE, DateType(), nullable=True),
            StructField(STATUS_CODE, IntegerType(), nullable=True),
            StructField(DATA_SOURCE, StringType(), nullable=True),
        ]
    )


class BookingPostProcessed:
    TABLE = "booking"

    TRANSACTION_ID = "transaction_id"
    GROUP_FLAG = "group_flag"
    BOOKING_CREATION_DATE = "booking_creation_date"
    PRICE_DRIVER_1 = "price_driver_1"
    NET_REVENUE = "net_revenue"

    schema = StructType(
        [
            StructField(TRANSACTION_ID, StringType(), nullable=False),
            StructField(GROUP_FLAG, IntegerType(), nullable=True),
            StructField(BOOKING_CREATION_DATE, DateType(), nullable=True),
            StructField(
                NET_REVENUE,
                DecimalType(12, 4),
                nullable=True,
            ),
        ]
    )


BOOKING_BRONZE_COLUMN_MAPPING = {
    BookingCSV.TRANSACTION_ID: BookingBronze.TRANSACTION_ID,
    BookingCSV.CREATE_DATE: BookingBronze.CREATE_DATE,
    BookingCSV.STATUS_CODE: BookingBronze.STATUS_CODE,
}
