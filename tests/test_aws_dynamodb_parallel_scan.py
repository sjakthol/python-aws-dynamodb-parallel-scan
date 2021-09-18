import itertools
import importlib
import operator
import unittest.mock

import boto3
import more_itertools
import pytest

import aws_dynamodb_parallel_scan
from .utils import generate_items

MOCK_TABLE_NAME = "dynamodb-parallel-scan-testtable"
MOCK_TABLE_ITEMS = generate_items(205)


@pytest.fixture()
def setup_env(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-north-1")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")


def mock_scan(**kwargs):
    """Mocked version of DynamoDB scan."""
    segments = list(
        more_itertools.divide(kwargs.get("TotalSegments", 1), MOCK_TABLE_ITEMS)
    )
    segment = list(segments[kwargs.get("Segment", 0)])

    limit = kwargs.get("Limit", 100)
    start = kwargs.get("ExclusiveStartKey", 0)
    items_to_return = segment[start : start + limit]

    response = {
        "Items": items_to_return,
        "Count": len(items_to_return),
        "ScannedCount": len(items_to_return),
    }

    if start + len(items_to_return) < len(segment):
        response["LastEvaluatedKey"] = start + len(items_to_return)

    return response


@pytest.fixture()
def mocked_client(setup_env):
    """Get DynamoDB client with mocked scan method."""
    client = boto3.client("dynamodb")
    with unittest.mock.patch.object(client, "scan", side_effect=mock_scan):
        yield client

    # Need to reload as fake creds we had in env would confuse boto3 for good otherwise
    importlib.reload(boto3)


@pytest.mark.parametrize(
    "scan_args",
    [
        {},
        dict(TotalSegments=4),
        dict(TotalSegments=25),
        dict(TotalSegments=4, Limit=100),
    ],
)
def test_parallel_scan(mocked_client, scan_args):
    paginator = aws_dynamodb_parallel_scan.get_paginator(mocked_client)
    pages = list(paginator.paginate(TableName=MOCK_TABLE_NAME, **scan_args))
    items = list(itertools.chain(*(page["Items"] for page in pages)))
    assert len(items) == 205
    assert sorted(items, key=operator.itemgetter("pk")) == sorted(
        MOCK_TABLE_ITEMS, key=operator.itemgetter("pk")
    )


def test_parallel_scan_with_break(mocked_client):
    paginator = aws_dynamodb_parallel_scan.get_paginator(mocked_client)
    for _ in paginator.paginate(TableName=MOCK_TABLE_NAME, TotalSegments=4, Limit=10):
        break

    assert mocked_client.scan.call_count == 4
