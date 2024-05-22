
# S3 to ClickHouse Data Ingestion

This script ingests data from S3 into ClickHouse based on configurations stored in a JSON file. It uses `argparse` to handle command-line arguments, making it flexible and easy to use.

## Prerequisites

1. Python 3.x
2. ClickHouse server running
3. S3 bucket with the data files

## Installation

1. Clone the repository or download the script.
2. Install the required Python packages:

   ```bash
   pip install -r requirements.txt
   ```

3. Create a `.env` file in the same directory as your script with the following content:

   ```plaintext
   AWS_ACCESS_KEY_ID=your_access_key_id
   AWS_SECRET_ACCESS_KEY=your_secret_access_key
   AWS_REGION=your_aws_region
   ```

## Configuration

Create a `config.json` file with the following structure:

```json
{
    "s3_sources": {
        "source1": {
            "s3_url": "https://s3.amazonaws.com/your-bucket-name/your-file1.csv",
            "table_name": "target_table1",
            "format": "CSV",
            "schema": "timestamp DateTime, user_id UInt32, event_type String, value Float32"
        },
        "source2": {
            "s3_url": "https://s3.amazonaws.com/your-bucket-name/your-file2.csv",
            "table_name": "target_table2",
            "format": "CSV",
            "schema": "timestamp DateTime, user_id UInt32, event_type String, value Float32"
        }
    }
}
```

## Usage

Run the script with the source key as an argument:

```bash
python ingest_from_s3.py source1
```

To specify a custom configuration file:

```bash
python ingest_from_s3.py source1 --config /path/to/your/custom_config.json
```

### Command-line Arguments

- `source_key`: The key for the S3 source in the config file.
- `--config`: Path to the configuration file (default is `config.json`).

## Example

```bash
python ingest_from_s3.py source1
```

This will read the configuration for `source1` from `config.json` and stream data from the specified S3 URL to the corresponding ClickHouse table.

## Conclusion

This script provides a flexible and efficient way to ingest data from S3 into ClickHouse, leveraging command-line arguments and configuration files for easy management and automation.
