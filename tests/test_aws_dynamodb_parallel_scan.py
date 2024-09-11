import itertools
import json
import operator
import unittest.mock

import pytest

import aws_dynamodb_parallel_scan

from . import utils

MOCK_TABLE_NAME = "dynamodb-parallel-scan-testtable"


@pytest.mark.parametrize(
    "scan_args",
    [
        {},
        dict(TotalSegments=4),
        dict(TotalSegments=25),
        dict(TotalSegments=4, Limit=100),
    ],
)
def test_parallel_scan_mocked_client(mocked_client, scan_args):
    paginator = aws_dynamodb_parallel_scan.get_paginator(mocked_client)
    items = utils.items_from_pages(paginator.paginate(TableName=MOCK_TABLE_NAME, **scan_args))
    assert len(items) == 205
    assert sorted(items, key=operator.itemgetter("pk")) == sorted(
        utils.generate_items(205), key=operator.itemgetter("pk")
    )


def test_parallel_scan_with_break(mocked_client):
    paginator = aws_dynamodb_parallel_scan.get_paginator(mocked_client)
    for _ in paginator.paginate(TableName=MOCK_TABLE_NAME, TotalSegments=4, Limit=10):
        break

    assert mocked_client.scan.call_count == 4


@pytest.mark.parametrize(
    "scan_args, returned_items",
    [
        ({}, 205),
        (
            dict(
                FilterExpression="attr2 < :mv",
                ExpressionAttributeValues={":mv": 10},
            ),
            10,
        ),
    ],
)
def test_parallel_scan_mocked_table(
    mocked_table,
    scan_args,
    returned_items,
):
    paginator = aws_dynamodb_parallel_scan.get_paginator(utils.dynamodb_document_client())
    items = utils.items_from_pages(paginator.paginate(TableName=MOCK_TABLE_NAME, **scan_args))
    assert len(items) == returned_items
    assert sorted(items, key=operator.itemgetter("pk")) == sorted(
        utils.generate_items(returned_items), key=operator.itemgetter("pk")
    )


@pytest.mark.parametrize(
    "extra_args",
    [
        [],
        ["--total-segments", "4"],
        ["--total-segments", "25"],
        ["--total-segments", "4", "--limit", "100"],
    ],
)
def test_cli_scan_mocked_client(mocked_client, extra_args, capsys):
    args = ["aws-dynamodb-parallel-scan", "--table-name", MOCK_TABLE_NAME] + extra_args
    with unittest.mock.patch("sys.argv", args):
        with unittest.mock.patch("boto3.client", return_value=mocked_client):
            aws_dynamodb_parallel_scan.cli()

    pages = [json.loads(line) for line in capsys.readouterr().out.split("\n") if line]
    items = list(itertools.chain(*(page["Items"] for page in pages)))

    assert len(items) == 205
    assert sorted(items, key=operator.itemgetter("pk")) == sorted(
        utils.generate_items(205), key=operator.itemgetter("pk")
    )


@pytest.mark.parametrize(
    "extra_args, output_deserializer, item_deserializer, returned_items",
    [
        ([], utils.items_from_pages_output, utils.deserialize_item, 205),
        (["--output-items"], utils.parse_jsonl, utils.deserialize_item, 205),
        (["--use-document-client"], utils.items_from_pages_output, utils.no_op, 205),
        (
            ["--use-document-client", "--output-items"],
            utils.parse_jsonl,
            utils.no_op,
            205,
        ),
        (
            [
                "--filter-expression",
                "attr2 < :mv",
                "--expression-attribute-values",
                """{":mv": {"N": "10"}}""",
            ],
            utils.items_from_pages_output,
            utils.deserialize_item,
            10,
        ),
        (
            [
                "--use-document-client",
                "--filter-expression",
                "attr2 < :mv",
                "--expression-attribute-values",
                """{":mv": 10}""",
            ],
            utils.items_from_pages_output,
            utils.no_op,
            10,
        ),
    ],
)
def test_cli_scan_mocked_table(
    mocked_table,
    capsys,
    extra_args,
    output_deserializer,
    item_deserializer,
    returned_items,
):
    args = ["aws-dynamodb-parallel-scan", "--table-name", MOCK_TABLE_NAME] + extra_args
    with unittest.mock.patch("sys.argv", args):
        aws_dynamodb_parallel_scan.cli()

    items = list(map(item_deserializer, output_deserializer(capsys.readouterr().out)))
    assert len(items) == returned_items
    assert sorted(items, key=operator.itemgetter("pk")) == sorted(
        utils.generate_items(returned_items), key=operator.itemgetter("pk")
    )
