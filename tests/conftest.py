import importlib
import unittest.mock

import boto3
import botocore.exceptions
import more_itertools
import moto  # type: ignore
import pytest

from . import utils

TEST_TABLE_NAME = "dynamodb-parallel-scan-testtable"
TEST_TABLE_ITEM_COUNT = 205
MOCK_SCAN_ITEMS = utils.generate_items(TEST_TABLE_ITEM_COUNT)


def mock_scan(**kwargs):
    """Mocked version of DynamoDB scan that supports parallel scanning."""
    segments = list(
        more_itertools.divide(kwargs.get("TotalSegments", 1), MOCK_SCAN_ITEMS)
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
def mock_aws_env(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-north-1")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")


@pytest.fixture()
def mocked_client(mock_aws_env):
    """Get DynamoDB client with mocked scan method."""
    client = boto3.client("dynamodb")
    with unittest.mock.patch.object(client, "scan", side_effect=mock_scan):
        yield client

    # Need to reload as fake creds we had in env would confuse boto3 for good otherwise
    importlib.reload(boto3)


@pytest.fixture()
def mocked_table(mock_aws_env):
    with moto.mock_aws():
        utils.create_test_table(TEST_TABLE_NAME)
        utils.fill_table(TEST_TABLE_NAME, TEST_TABLE_ITEM_COUNT)
        yield

    # Need to reload as fake creds we had in env would confuse boto3 for good otherwise
    importlib.reload(boto3)


@pytest.fixture(scope="module")
def real_table():
    try:
        utils.create_test_table(TEST_TABLE_NAME)
        utils.fill_table(TEST_TABLE_NAME, TEST_TABLE_ITEM_COUNT)
    except (botocore.exceptions.BotoCoreError, botocore.exceptions.ClientError) as exc:
        pytest.skip("Failed to create table, skipping integration test (%s)" % exc)
        return

    yield

    utils.delete_table(TEST_TABLE_NAME)
