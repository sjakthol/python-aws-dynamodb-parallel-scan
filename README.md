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

## CLI

This package also provides a CLI tool (`aws-dynamodb-parallel-scan`) to scan a DynamoDB table
with parallel scan. The tool supports all non-deprecated arguments of DynamoDB Scan API. Execute
`aws-dynamodb-parallel-scan -h` for details

Here's some examples:

```bash
# Scan "mytable" sequentially
$ aws-dynamodb-parallel-scan --table-name mytable
{"Items": [...], "Count": 10256, "ScannedCount": 10256, "ResponseMetadata": {}}
{"Items": [...], "Count": 12, "ScannedCount": 12, "ResponseMetadata": {}}

# Scan "mytable" in parallel (5 parallel segments)
$ aws-dynamodb-parallel-scan --table-name mytable --total-segments 5
{"Items": [...], "Count":32, "ScannedCount":32, "ResponseMetadata": {}}
{"Items": [...], "Count":47, "ScannedCount":47, "ResponseMetadata": {}}
{"Items": [...], "Count":52, "ScannedCount":52, "ResponseMetadata": {}}
{"Items": [...], "Count":34, "ScannedCount":34, "ResponseMetadata": {}}
{"Items": [...], "Count":40, "ScannedCount":40, "ResponseMetadata": {}}

# Scan "mytable" in parallel and return items, not Scan API responses (--output-items flag)
$ aws-dynamodb-parallel-scan --table-name mytable --total-segments 5 \
    --output-items
{"pk": {"S": "item1"}, "quantity": {"N": "99"}}
{"pk": {"S": "item24"}, "quantity": {"N": "25"}}
...

# Scan "mytable" in parallel, return items with native types, not DynamoDB types (--use-document-client flag)
$ aws-dynamodb-parallel-scan --table-name mytable --total-segments 5 \
    --output-items --use-document-client
{"pk": "item1", "quantity": 99}
{"pk": "item24", "quantity": 25}
...

# Scan "mytable" with a filter expression, return items
$ aws-dynamodb-parallel-scan --table-name mytable --total-segments 5 \
    --filter-expression "quantity < :value" \
    --expression-attribute-values '{":value": {"N": "5"}}' \
    --output-items
{"pk": {"S": "item142"}, "quantity": {"N": "4"}}
{"pk": {"S": "item874"}, "quantity": {"N": "1"}}

# Scan "mytable" with a filter expression using native types, return items
$ aws-dynamodb-parallel-scan --table-name mytable --total-segments 5 \
    --filter-expression "quantity < :value" \
    --expression-attribute-values '{":value": 5}' \
    --use-document-client --output-items
{"pk": "item142", "quantity": 4}
{"pk": "item874", "quantity": 1}
```

## Development

Requires Python 3 and Poetry. Useful commands:

```bash
# Setup environment
poetry install

# Run tests (integration test requires rights to create, delete and use DynamoDB tables)
make test

# Run linters
make -k lint

# Format code
make format
```

## License

MIT

## Credits

* Alex Chan, [Getting every item from a DynamoDB table with Python](https://alexwlchan.net/2020/05/getting-every-item-from-a-dynamodb-table-with-python/)
