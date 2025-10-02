import os
import json
import boto3
from botocore.exceptions import ClientError


def get_secret(secret_name: str = None, region_name: str = None):
    """Get secret from AWS Secrets Manager and return as dict"""

    # Use environment variables or defaults
    secret_name = secret_name or os.environ.get(
        "AWS_SECRETS_NAME", "job-pipeline/secrets"
    )
    region_name = region_name or os.environ.get("AWS_REGION", "eu-west-2")

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        print(f"Error retrieving secret: {e}")
        return {}

    secret_string = get_secret_value_response["SecretString"]

    # Parse JSON and return as dict
    try:
        return json.loads(secret_string)
    except json.JSONDecodeError as e:
        print(f"Error parsing secret JSON: {e}")
        return {}


def load_secrets_to_env():
    """Load secrets from AWS and set as environment variables"""
    secrets = get_secret()

    # Set as environment variables (only if not already set)
    for key, value in secrets.items():
        if key not in os.environ:
            os.environ[key] = value

    return secrets
