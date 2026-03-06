import json
import os
import sys

import boto3
from botocore.exceptions import ClientError

# Static configuration
MAX_RETRIES = 3
BATCH_SIZE = 20_000
RETRY_WAIT_SECONDS = 5
VALID_ENVIRONMENTS = ("local", "docker", "dev", "test", "prod")
DB_DRIVER = "org.postgresql.Driver"

# env vars
DB_NAME = os.environ.get("DB_NAME", "focl_local_db")
DB_USER = os.environ.get("DB_USER", "focl_local_user")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "focl_local_pass")
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", "5430")


def get_aws_secrets(secret_name: str, region_name: str) -> dict:
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        print(f"Error fetching secret: {e}")
        raise

    secret = get_secret_value_response["SecretString"]
    return json.loads(secret)


def get_environment() -> str:
    env = os.environ.get("ENVIRONMENT", "local")
    if env not in VALID_ENVIRONMENTS:
        raise Exception("Invalid environment: %s. Falling back to default `local`", env)
    return env


def fetch_glue_param(param: str) -> str:
    """
    Tries to fetch Glue parameter.
    On fail, assumes local environment and looks for environment variable with same name.
    """
    fallback = os.getenv(param)

    try:
        from awsglue.utils import getResolvedOptions

        args = getResolvedOptions(
            sys.argv,
            [param],
        )
        return args[param]

    except ImportError as e:
        print(f"Warning: awsglue.utils not found, using fallback '{fallback}': {e}")

    except Exception as e:
        print(f"Warning: Could not parse Glue arguments, using fallback {fallback}: {e}")
    return fallback


ENVIRONMENT = get_environment()
# if applicable, fetch more config parameters here using above functions
