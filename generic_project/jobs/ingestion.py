"""
Dummy Job, to illustrate the general structure of a job. In this case its designed as a glue job, but with little
changes this could of course also be deployed in other environments.

A module such as "Booking" could have another job besides "BookingIngestions" - for example "BookingPostProcessing".
This would be another implementation of the ETL Base class and likely be called in a separate post-processing job.
"""

import sys
import traceback

import boto3
from awsglue.utils import getResolvedOptions
from common.session_management import get_default_spark_or_glue_session
from modules.booking.ingestion import BookingIngestion, OccupancyIngestion

args = getResolvedOptions(sys.argv, ["JOB_NAME", "WORKFLOW_NAME", "WORKFLOW_RUN_ID"])
glue_context, spark = get_default_spark_or_glue_session("ingestion")

glue_client = boto3.client("glue")
workflow_name = args["WORKFLOW_NAME"]
workflow_run_id = args["WORKFLOW_RUN_ID"]

workflow_params = glue_client.get_workflow_run_properties(Name=workflow_name, RunId=workflow_run_id)["RunProperties"]

try:
    if "booking_path" in workflow_params:
        BookingIngestion(
            booking_path=workflow_params["booking_path"],
            glue_context=glue_context,
            spark=spark,
        ).run()
        print("✅ Booking completed")

    if "occupancy_path" in workflow_params:
        OccupancyIngestion(
            occupancy_path=workflow_params["occupancy_path"],
            glue_context=glue_context,
            spark=spark,
        ).run()
        print("✅ Occupancy (Cruise Capacity) completed")

    print("\nIngestion batch completed!")

except Exception as e:
    print(f"❌ Ingestion pipeline failed: {e}")
    print(f"x Following workflow parameters were used: {workflow_params}")
    traceback.print_exc()
    raise
