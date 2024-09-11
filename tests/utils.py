"""Helpers for tests."""

import itertools
import json
import logging
from decimal import Decimal
from typing import Iterable

import boto3
import boto3.dynamodb.types
import more_itertools
from mypy_boto3_dynamodb.client import DynamoDBClient
from mypy_boto3_dynamodb.type_defs import ScanOutputTypeDef, WaiterConfigTypeDef

DDB_DESERIALIZER = boto3.dynamodb.types.TypeDeserializer()
WAITER_CONFIG: WaiterConfigTypeDef = {"Delay": 2, "MaxAttempts": 30}


def deserialize_item(item: dict):
    """Deserialize DynamoDB item to Python types.

    Args:
        item: item to deserialize

    Return: deserialized item
    """
    return {k: DDB_DESERIALIZER.deserialize(v) for k, v in item.items()}


def generate_item(i: int):
    """Generate DynamoDB item.

    Args:
        i: Item identifier.

    Returns:
        A DynamoDB object of form { "pk": i, "attr1": "test", "attr2": i }.
    """
    return {"pk": str(i), "attr1": "test", "attr2": i, "attr3": Decimal(1) / Decimal(2)}


def generate_items(n: int):
    """Generate N items with IDs from 0 to N.

    Args:
        n: Number of items to generate.

    Returns:
        List of item objects of form { "pk": i, "attr1": "test", "attr2": i }
        for i in range(n).
    """
    return [generate_item(i) for i in range(n)]


def create_test_table(table_name: str):
    logging.info("Creating table %s", table_name)
    client = boto3.client("dynamodb")
    client.create_table(
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
        ],
        BillingMode="PROVISIONED",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        ProvisionedThroughput={
            "ReadCapacityUnits": 100,
            "WriteCapacityUnits": 100,
        },
        TableName=table_name,
    )
    client.get_waiter("table_exists").wait(TableName=table_name, WaiterConfig=WAITER_CONFIG)


def fill_table(table_name: str, n: int):
    """Write N items to given table.

    Args:
        table_name: Target table.
        n: Number of items to create.

    """
    logging.info("Filling table %s with %i items", table_name, n)
    client = boto3.resource("dynamodb").meta.client
    for batch in more_itertools.chunked(generate_items(n), 25):
        client.batch_write_item(RequestItems={table_name: [{"PutRequest": {"Item": item}} for item in batch]})


def delete_table(table_name):
    logging.info("Deleting table %s", table_name)
    client = boto3.client("dynamodb")
    client.delete_table(TableName=table_name)

    logging.info("Waiting for deletion to complete")
    client.get_waiter("table_not_exists").wait(TableName=table_name, WaiterConfig=WAITER_CONFIG)


def no_op(v):
    """Returns input as-is."""
    return v


def parse_jsonl(text: str):
    """Parse given text as newline delimited JSON.

    Args:
        text: Newline delimited JSON to parse

    Returns:
        List of parsed JSON objects.
    """
    return [json.loads(line) for line in text.split("\n") if line]


def items_from_pages_output(output: str):
    """Get all items from given Scan API result pages encoded as newline delimited JSON.

    Args:
        output: newline delimited JSON string with a Scan API response on each line.
    """
    return items_from_pages(parse_jsonl(output))


def items_from_pages(pages: Iterable[ScanOutputTypeDef]):
    """Get all items from given Scan API result pages.

    Args:
        pages: Scan API responses.

    Returns:
        DynamoDB items.
    """
    return list(itertools.chain(*(page["Items"] for page in pages)))


def dynamodb_client() -> DynamoDBClient:
    """Get low-level DynamoDB client."""
    return boto3.client("dynamodb")


def dynamodb_document_client() -> DynamoDBClient:
    """Get low-level DynamoDB client with automatic type conversion."""
    return boto3.resource("dynamodb").meta.client
