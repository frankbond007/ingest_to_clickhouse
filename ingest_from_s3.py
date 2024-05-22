import json
import requests
import argparse
import os
from clickhouse_driver import Client
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

# Load environment variables from .env file
load_dotenv()

# Configuration for ClickHouse connection
CLICKHOUSE_HOST = 'localhost'
CLICKHOUSE_PORT = 9000
CLICKHOUSE_USER = 'default'
CLICKHOUSE_PASSWORD = ''
CLICKHOUSE_DB = 'default'

# S3 Credentials from environment variables
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')

# ClickHouse client
client = Client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    user=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
    database=CLICKHOUSE_DB
)


def stream_data_from_s3_to_clickhouse(client, s3_url, table_name, format):
    """Stream data from S3 to ClickHouse."""
    response = requests.get(s3_url, stream=True, auth=HTTPBasicAuth(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY))
    response.raise_for_status()
    client.execute(
        f'INSERT INTO {table_name} FORMAT {format}', response.iter_lines()
    )


def main(source_key, config_file):
    # Load the configuration from the JSON file
    with open(config_file, 'r') as f:
        config = json.load(f)

    # Get the source configuration from the config file
    source = config['s3_sources'].get(source_key)
    if not source:
        print(f"Source key '{source_key}' not found in configuration.")
        return

    s3_url = source['s3_url']
    table_name = source['table_name']
    format = source['format']

    # Stream data from S3 to ClickHouse
    stream_data_from_s3_to_clickhouse(client, s3_url, table_name, format)
    print(f"Data streamed from {s3_url} to ClickHouse table {table_name}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Ingest data from S3 to ClickHouse.")
    parser.add_argument('source_key', type=str, help='The key for the S3 source in the config file.')
    parser.add_argument('--config', type=str, default='config.json', help='Path to the configuration file.')

    args = parser.parse_args()
    main(args.source_key, args.config)