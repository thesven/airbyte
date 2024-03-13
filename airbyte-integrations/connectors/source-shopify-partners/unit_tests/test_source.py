#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from unittest.mock import MagicMock

from source_shopify_partners.source import SourceShopifyPartners


def test_check_connection(mocker):
    source = SourceShopifyPartners()
    logger_mock, config_mock = MagicMock(), MagicMock()
    assert source.check_connection(logger_mock, config_mock) == (False,)


def test_streams(mocker):
    source = SourceShopifyPartners()
    config_mock = MagicMock()
    streams = source.streams(config_mock)
    # TODO: replace this with your streams number
    expected_streams_number = 5
    assert len(streams) == expected_streams_number
