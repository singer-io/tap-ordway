from unittest import TestCase
from unittest.mock import MagicMock, patch
from tap_ordway.property import (
    get_key_properties,
    get_replication_key,
    get_replication_method,
    get_stream_metadata,
)
from ..utils import load_schema


def test_get_key_properties():
    foo_stream = MagicMock()
    foo_stream.key_properties = ["template_id"]
    bar_stream = MagicMock()
    bar_stream.key_properties = []

    with patch.dict(
        "tap_ordway.property.AVAILABLE_STREAMS",
        {"foo": foo_stream, "bar": bar_stream},
        clear=True,
    ):
        assert get_key_properties("foo") == ["template_id"]
        assert get_key_properties("bar") is None


def test_get_replication_key():
    foo_stream = MagicMock()
    foo_stream.replication_key = "updated_date"
    bar_stream = MagicMock()
    bar_stream.replication_key = None

    with patch.dict(
        "tap_ordway.property.AVAILABLE_STREAMS",
        {"foo": foo_stream, "bar": bar_stream},
        clear=True,
    ):
        assert get_replication_key("foo") == "updated_date"
        assert get_replication_key("bar") is None


def test_get_replication_method():
    foo_stream = MagicMock()
    foo_stream.replication_method = "FULL_TABLE"
    bar_stream = MagicMock()
    bar_stream.replication_method = "INCREMENTAL"

    with patch.dict(
        "tap_ordway.property.AVAILABLE_STREAMS",
        {"foo": foo_stream, "bar": bar_stream},
        clear=True,
    ):
        assert get_replication_method("foo") == "FULL_TABLE"
        assert get_replication_method("bar") == "INCREMENTAL"


class _TestStream:
    valid_replication_keys = []
    key_properties = ["name"]
    replication_method = "FULL_TABLE"


class GetStreamMetadataTestCase(TestCase):
    @patch.dict(
        "tap_ordway.property.AVAILABLE_STREAMS",
        {"foo": _TestStream},
        clear=True,
    )
    def test_name_in_key_properties(self):
        mdata = get_stream_metadata("foo", load_schema("webhooks.json"))

        self.assertIn("table-key-properties", mdata[0]["metadata"])
        self.assertListEqual(mdata[0]["metadata"]["table-key-properties"], ["name"])

    @patch.dict(
        "tap_ordway.property.AVAILABLE_STREAMS",
        {"foo": _TestStream},
        clear=True,
    )
    def test_forced_replication_method_included(self):
        """Test that forced-replication-method is included in metadata (PR #27)"""
        mdata = get_stream_metadata("foo", load_schema("webhooks.json"))
        
        # Find the table-level metadata
        table_metadata = None
        for entry in mdata:
            if entry["breadcrumb"] == ():
                table_metadata = entry["metadata"]
                break
        
        self.assertIsNotNone(table_metadata, "Table-level metadata should exist")
        self.assertIn("forced-replication-method", table_metadata)
        self.assertEqual(table_metadata["forced-replication-method"], "FULL_TABLE")


class _TestStreamWithParent:
    valid_replication_keys = []
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    parent = "customers"


class _TestStreamWithoutParent:
    valid_replication_keys = []
    key_properties = ["id"]
    replication_method = "FULL_TABLE"


class ParentStreamMetadataTestCase(TestCase):
    """Test parent-tap-stream-id functionality added in PR #27"""
    
    @patch.dict(
        "tap_ordway.property.AVAILABLE_STREAMS",
        {"child_stream": _TestStreamWithParent},
        clear=True,
    )
    def test_parent_tap_stream_id_included(self):
        """Test that parent-tap-stream-id is included for substreams"""
        mdata = get_stream_metadata("child_stream", load_schema("webhooks.json"))
        
        # Find the table-level metadata
        table_metadata = None
        for entry in mdata:
            if entry["breadcrumb"] == ():
                table_metadata = entry["metadata"]
                break
        
        self.assertIsNotNone(table_metadata, "Table-level metadata should exist")
        self.assertIn("parent-tap-stream-id", table_metadata)
        self.assertEqual(table_metadata["parent-tap-stream-id"], "customers")

    @patch.dict(
        "tap_ordway.property.AVAILABLE_STREAMS",
        {"parent_stream": _TestStreamWithoutParent},
        clear=True,
    )
    def test_parent_tap_stream_id_not_included_for_parent_streams(self):
        """Test that parent-tap-stream-id is not included for parent streams"""
        mdata = get_stream_metadata("parent_stream", load_schema("webhooks.json"))
        
        # Find the table-level metadata
        table_metadata = None
        for entry in mdata:
            if entry["breadcrumb"] == ():
                table_metadata = entry["metadata"]
                break
        
        self.assertIsNotNone(table_metadata, "Table-level metadata should exist")
        self.assertNotIn("parent-tap-stream-id", table_metadata)
