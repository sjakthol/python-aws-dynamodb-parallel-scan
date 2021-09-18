import itertools
import logging
import operator

import boto3
import botocore.exceptions
import more_itertools
import pytest

import aws_dynamodb_parallel_scan

from .utils import generate_items

TEST_TABLE_NAME = "dynamodb-parallel-scan-testtable"
WAITER_CONFIG = {"Delay": 2, "MaxAttempts": 30}

logging.getLogger("botocore").setLevel(logging.INFO)


@pytest.fixture(scope="module")
def real_table():
    try:
        logging.info("Creating table %s", TEST_TABLE_NAME)
        ddb = boto3.client("dynamodb")
        ddb.create_table(
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
            ],
            BillingMode="PROVISIONED",
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            ProvisionedThroughput={
                "ReadCapacityUnits": 100,
                "WriteCapacityUnits": 100,
            },
            TableName=TEST_TABLE_NAME,
        )
    except (botocore.exceptions.BotoCoreError, botocore.exceptions.ClientError) as exc:
        pytest.skip("Failed to create table, skipping integration test (%s)" % exc)
        return

    logging.info("Waiting for table to be ready...")
    ddb.get_waiter("table_exists").wait(
        TableName=TEST_TABLE_NAME, WaiterConfig=WAITER_CONFIG
    )

    logging.info("Table ready. Populating table...")
    client = boto3.resource("dynamodb").meta.client
    for batch in more_itertools.chunked(generate_items(501), 25):
        client.batch_write_item(
            RequestItems={
                TEST_TABLE_NAME: [{"PutRequest": {"Item": item}} for item in batch]
            }
        )

    logging.info("Table populated. Running tests...")
    yield

    logging.info("Deleting table %s", TEST_TABLE_NAME)
    ddb.delete_table(TableName=TEST_TABLE_NAME)

    logging.info("Waiting for deletion to complete...")
    ddb.get_waiter("table_not_exists").wait(
        TableName=TEST_TABLE_NAME, WaiterConfig=WAITER_CONFIG
    )


@pytest.mark.parametrize(
    ("scan_args", "returned_items"),
    [
        ({}, 501),
        (dict(TotalSegments=4), 501),
        (dict(TotalSegments=4, Segment=1), 501),
        (dict(TotalSegments=25), 501),
        (dict(TotalSegments=4, Limit=100), 501),
        (
            dict(
                TotalSegments=4,
                Limit=100,
                FilterExpression="attr2 < :val",
                ExpressionAttributeValues={":val": 10},
            ),
            10,
        ),
    ],
)
def test_integration(real_table, scan_args, returned_items):
    logging.info("Scanning table with args %s", scan_args)
    client = boto3.resource("dynamodb").meta.client
    paginator = aws_dynamodb_parallel_scan.get_paginator(client)
    pages = list(paginator.paginate(TableName=TEST_TABLE_NAME, **scan_args))
    items = list(itertools.chain(*(page["Items"] for page in pages)))

    assert len(items) == returned_items
    assert sorted(items, key=operator.itemgetter("pk")) == sorted(
        generate_items(returned_items), key=operator.itemgetter("pk")
    )
