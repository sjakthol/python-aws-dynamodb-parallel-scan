import logging
import operator
import unittest.mock

import pytest

import aws_dynamodb_parallel_scan

from . import utils

TEST_TABLE_NAME = "dynamodb-parallel-scan-testtable"

logging.getLogger("botocore").setLevel(logging.INFO)


@pytest.mark.parametrize(
    ("scan_args", "returned_items"),
    [
        ({}, 205),
        (dict(TotalSegments=4), 205),
        (dict(TotalSegments=4, Segment=1), 205),
        (dict(TotalSegments=25), 205),
        (dict(TotalSegments=4, Limit=100), 205),
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
    paginator = aws_dynamodb_parallel_scan.get_paginator(utils.dynamodb_document_client())
    items = utils.items_from_pages(paginator.paginate(TableName=TEST_TABLE_NAME, **scan_args))

    assert len(items) == returned_items
    assert sorted(items, key=operator.itemgetter("pk")) == sorted(
        utils.generate_items(returned_items), key=operator.itemgetter("pk")
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
def test_integration_cli_scan(
    real_table,
    capsys,
    extra_args,
    output_deserializer,
    item_deserializer,
    returned_items,
):
    args = ["aws-dynamodb-parallel-scan", "--table-name", TEST_TABLE_NAME] + extra_args
    with unittest.mock.patch("sys.argv", args):
        aws_dynamodb_parallel_scan.cli()

    items = list(map(item_deserializer, output_deserializer(capsys.readouterr().out)))
    assert len(items) == returned_items
    assert sorted(items, key=operator.itemgetter("pk")) == sorted(
        utils.generate_items(returned_items), key=operator.itemgetter("pk")
    )
