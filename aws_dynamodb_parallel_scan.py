"""DynamoDB parallel scan paginator for boto3.

Adapted from parallel scan implementation of Alex Chan (MIT license):
https://alexwlchan.net/2020/05/getting-every-item-from-a-dynamodb-table-with-python/
"""

import argparse
import concurrent.futures
import decimal
import json
import textwrap
import typing

import boto3

if typing.TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_dynamodb import DynamoDBClient
else:
    DynamoDBClient = None  # pylint: disable=invalid-name


class Paginator:  # pylint: disable=too-few-public-methods
    """Paginator that implements DynamoDB parallel scan.

    Similar to boto3 DynamoDB scan paginator but scans the table in
    parallel.
    """

    def __init__(self, client: DynamoDBClient):
        """Create paginator for DynamoDB parallel scan.

        Args:
            client: DynamoDB client to use for Scan API calls.
        """
        self._client = client

    def paginate(self, **kwargs):
        # pylint: disable=line-too-long
        """Creates a generator that yields DynamoDB Scan API responses.

        paginate() accepts the same arguments as boto3 DynamoDB.Client.scan() method. Arguments
        are passed to DynamoDB.Client.scan() as-is.

        paginate() uses the value of TotalSegments argument as parallelism level. Each segment
        is scanned in parallel in a separate thread.

        paginate() yields DynamoDB Scan API responses boto3 DynamoDB.Paginator.Scan.paginate()
        method.

        See boto3 DynamoDB.Client.scan documentation (https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.scan)
        for details on supported arguments and the response format.
        """
        # pylint: enable=line-too-long
        segments = kwargs.get("TotalSegments") or 1
        with concurrent.futures.ThreadPoolExecutor(max_workers=segments) as executor:

            # Prepare Scan arguments for each segment of the parallel scan.
            tasks = (
                {**kwargs, "TotalSegments": segments, "Segment": i}
                for i in range(segments)
            )

            # Submit scan operation for each segment
            futures = {executor.submit(self._client.scan, **t): t for t in tasks}

            while futures:
                # Collect results
                done, _ = concurrent.futures.wait(
                    futures, return_when=concurrent.futures.FIRST_COMPLETED
                )

                for future in done:
                    # Get the result and the scan args for the completed operation
                    task = futures.pop(future)
                    page = future.result()
                    yield page

                    next_key = page.get("LastEvaluatedKey")
                    if next_key:
                        # Still more items in this segment. Submit another scan operation for this
                        # segment that continues where the last one left off.
                        futures[
                            executor.submit(
                                self._client.scan,
                                **{**task, "ExclusiveStartKey": next_key}
                            )
                        ] = task


def get_paginator(client: DynamoDBClient):
    """Create paginator for DynamoDB parallel scan.

    Args:
        client: DynamoDB client to use for Scan API calls.

    Returns: Paginator object.
    """
    return Paginator(client)


def cli():
    """Entrypoint for CLI tool."""

    def json_value(value: str):
        return json.loads(value)

    class DecimalEncoder(json.JSONEncoder):
        """JSON encoder for DynamoDB Decimal types."""

        def default(self, o):
            if isinstance(o, decimal.Decimal):
                if abs(o) % 1 > 0:
                    return float(o)
                return int(o)
            return super().default(o)  # pragma: nocover

    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent(
            """
            Perform a parallel scan of DynamoDB table.

            Command line arguments map one-to-one to DynamoDB Scan request arguments. See
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.scan
            for reference.
            """
        ),
        epilog="Results are written to stdout as JSON objects (one per line).",
    )
    parser.add_argument(
        "--table-name", dest="TableName", metavar="<value>", required=True
    )
    parser.add_argument("--index-name", dest="IndexName", metavar="<value>")
    parser.add_argument("--limit", dest="Limit", metavar="<value>", type=int)
    parser.add_argument(
        "--return-consumed-capacity",
        dest="ReturnConsumedCapacity",
        metavar="<value>",
    )
    parser.add_argument(
        "--total-segments", dest="TotalSegments", metavar="<value>", type=int
    )
    parser.add_argument(
        "--projection-expression",
        dest="ProjectionExpression",
        metavar="<value>",
    )
    parser.add_argument(
        "--filter-expression", dest="FilterExpression", metavar="<value>"
    )
    parser.add_argument("--consistent-read", dest="ConsistentRead", action="store_true")
    parser.add_argument(
        "--expression-attribute-names",
        dest="ExpressionAttributeNames",
        metavar="<value>",
        type=json_value,
        help="""ExpressionAttributeNames as JSON string (e.g. {"#P":"Percentile"})""",
    )
    parser.add_argument(
        "--expression-attribute-values",
        dest="ExpressionAttributeValues",
        metavar="<value>",
        type=json_value,
        help="""ExpressionAttributeValues as JSON string (e.g. {":variable": {"S": "sample"}})""",
    )

    parser.add_argument(
        "--use-document-client",
        action="store_true",
        help="Use a document client that converts DynamoDB types to native types automatically.",
    )
    parser.add_argument(
        "--output-items",
        action="store_true",
        help="Output returned items, not full Scan API responses",
    )
    args = vars(parser.parse_args())

    output_items = args.pop("output_items", False)
    use_document_client = args.pop("use_document_client", False)
    scan_args = {k: v for k, v in args.items() if v is not None}

    client = (
        boto3.client("dynamodb")
        if not use_document_client
        else boto3.resource("dynamodb").meta.client
    )
    paginator = get_paginator(client)
    for page in paginator.paginate(**scan_args):
        if output_items:
            for item in page.get("Items", []):
                print(json.dumps(item, cls=DecimalEncoder))
        else:
            print(json.dumps(page, cls=DecimalEncoder))


if __name__ == "__main__":  # pragma: no cover
    cli()
