"""Integration tests for substreams that now have parent relationships."""

from unittest.mock import patch
from singer.messages import SchemaMessage
from base import BaseOrdwayTestCase


class SubstreamsWithParentsTestCase(BaseOrdwayTestCase):
    """Test case for substreams that have been updated to include parent relationships."""
    
    START_DATE = "2020-10-16"
    # Test the specific streams that were updated in the PR with parent relationships
    SELECTED_STREAMS = ("customers", "contacts", "payment_methods", "customer_notes", "plans", "charges")

    @classmethod
    def setUpClass(cls):
        with patch("tap_ordway.utils.time", return_value=123):
            super().setUpClass()

    def test_contacts_has_customers_parent(self):
        """Test that contacts stream has customers as parent-tap-stream-id."""
        self._assert_stream_has_parent("contacts", "customers")

    def test_payment_methods_has_customers_parent(self):
        """Test that payment_methods stream has customers as parent-tap-stream-id."""
        self._assert_stream_has_parent("payment_methods", "customers")

    def test_customer_notes_has_customers_parent(self):
        """Test that customer_notes stream has customers as parent-tap-stream-id."""
        self._assert_stream_has_parent("customer_notes", "customers")

    def test_charges_has_plans_parent(self):
        """Test that charges stream has plans as parent-tap-stream-id."""
        self._assert_stream_has_parent("charges", "plans")

    def test_customers_has_no_parent(self):
        """Test that customers stream (parent stream) has no parent-tap-stream-id."""
        self._assert_stream_has_no_parent("customers")

    def test_plans_has_no_parent(self):
        """Test that plans stream (parent stream) has no parent-tap-stream-id."""
        self._assert_stream_has_no_parent("plans")

    def _assert_stream_has_parent(self, stream_name, expected_parent):
        """Helper method to assert a stream has the expected parent-tap-stream-id."""
        schema_msg = self._get_schema_message_for_stream(stream_name)
        table_metadata = self._get_table_metadata(schema_msg)
        
        self.assertIn('parent-tap-stream-id', table_metadata,
                     f"Stream '{stream_name}' should have parent-tap-stream-id in metadata")
        
        actual_parent = table_metadata['parent-tap-stream-id']
        self.assertEqual(actual_parent, expected_parent,
                        f"Stream '{stream_name}' should have parent '{expected_parent}', "
                        f"but has '{actual_parent}'")

    def _assert_stream_has_no_parent(self, stream_name):
        """Helper method to assert a stream has no parent-tap-stream-id."""
        schema_msg = self._get_schema_message_for_stream(stream_name)
        table_metadata = self._get_table_metadata(schema_msg)
        
        self.assertNotIn('parent-tap-stream-id', table_metadata,
                        f"Stream '{stream_name}' should not have parent-tap-stream-id in metadata")

    def _get_schema_message_for_stream(self, stream_name):
        """Helper method to get the schema message for a specific stream."""
        schema_messages = list(
            self.tap_executor.output.filter_messages(message_type=SchemaMessage)
        )
        
        for msg in schema_messages:
            if msg.stream == stream_name:
                return msg
        
        self.fail(f"No schema message found for stream '{stream_name}'")

    def _get_table_metadata(self, schema_msg):
        """Helper method to extract table-level metadata from a schema message."""
        metadata_entries = schema_msg.schema.get('metadata', [])
        
        for entry in metadata_entries:
            if entry.get('breadcrumb') == [] or entry.get('breadcrumb') == ():
                return entry.get('metadata', {})
        
        self.fail(f"No table-level metadata found for stream '{schema_msg.stream}'")

    def test_all_substreams_maintain_existing_metadata(self):
        """Test that substreams still have their existing required metadata fields."""
        
        substreams_and_parents = {
            "contacts": "customers",
            "payment_methods": "customers", 
            "customer_notes": "customers",
            "charges": "plans"
        }
        
        for substream, parent in substreams_and_parents.items():
            with self.subTest(substream=substream):
                schema_msg = self._get_schema_message_for_stream(substream)
                table_metadata = self._get_table_metadata(schema_msg)
                
                # Check that existing required metadata is still present
                required_fields = [
                    'table-key-properties',
                    'inclusion'
                ]
                
                for field in required_fields:
                    self.assertIn(field, table_metadata,
                                f"Required metadata field '{field}' missing from substream '{substream}'")
                
                # Verify the parent relationship is correct
                self.assertEqual(table_metadata.get('parent-tap-stream-id'), parent,
                               f"Incorrect parent relationship for substream '{substream}'")

    def test_schema_messages_present_for_all_streams(self):
        """Test that schema messages are generated for all selected streams."""
        schema_messages = list(
            self.tap_executor.output.filter_messages(message_type=SchemaMessage)
        )
        
        stream_names = {msg.stream for msg in schema_messages}
        
        for expected_stream in self.SELECTED_STREAMS:
            self.assertIn(expected_stream, stream_names,
                         f"Schema message missing for stream '{expected_stream}'")

    def test_metadata_structure_integrity(self):
        """Test that the metadata structure remains intact after adding parent relationships."""
        schema_messages = list(
            self.tap_executor.output.filter_messages(message_type=SchemaMessage)
        )
        
        for schema_msg in schema_messages:
            with self.subTest(stream=schema_msg.stream):
                # Verify metadata is a list
                metadata = schema_msg.schema.get('metadata', [])
                self.assertIsInstance(metadata, list, 
                                    f"Metadata should be a list for stream '{schema_msg.stream}'")
                
                # Verify each metadata entry has required structure
                for entry in metadata:
                    self.assertIn('breadcrumb', entry,
                                f"Metadata entry missing 'breadcrumb' for stream '{schema_msg.stream}'")
                    self.assertIn('metadata', entry,
                                f"Metadata entry missing 'metadata' for stream '{schema_msg.stream}'")
                    
                    # Verify breadcrumb is a list
                    self.assertIsInstance(entry['breadcrumb'], list,
                                        f"Breadcrumb should be a list for stream '{schema_msg.stream}'")
                    
                    # Verify metadata is a dict
                    self.assertIsInstance(entry['metadata'], dict,
                                        f"Metadata should be a dict for stream '{schema_msg.stream}'")
