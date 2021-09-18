# aws-dynamodb-parallel-scan

Amazon DynamoDB parallel scan paginator for boto3.

## Installation

Install from PyPI with pip

```
pip install aws-dynamodb-parallel-scan
```

or with the package manager of choice.

## Usage

The library is a drop-in replacement for [boto3 DynamoDB Scan Paginator](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Paginator.Scan). Example:

```python
import aws_dynamodb_parallel_scan
import boto3

# Create DynamoDB client to use for scan operations
client = boto3.resource("dynamodb").meta.client

# Create the parallel scan paginator with the client
paginator = aws_dynamodb_parallel_scan.get_paginator(client)

# Scan "mytable" in five segments. Each segment is scanned in parallel.
for page in paginator.paginate(TableName="mytable", TotalSegments=5):
    items = page.get("Items", [])
```

Notes:

* `paginate()` accepts the same arguments as boto3 `DynamoDB.Client.scan()` method. Arguments
  are passed to `DynamoDB.Client.scan()` as-is.

* `paginate()` uses the value of `TotalSegments` argument as parallelism level. Each segment
  is scanned in parallel in a separate thread.

* `paginate()` yields DynamoDB Scan API responses in the same format as boto3
  `DynamoDB.Client.scan()` method.

See boto3 [DynamoDB.Client.scan() documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.scan)
for details on supported arguments and the response format.


## Development

Requires Python 3 and Poetry. Useful commands:

```bash
# Run tests
poetry run tox -e test

# Run linters
poetry run tox -e lint

# Format code
poetry run tox -e format
```

## License

MIT

## Credits

* Alex Chan, [Getting every item from a DynamoDB table with Python](https://alexwlchan.net/2020/05/getting-every-item-from-a-dynamodb-table-with-python/)
