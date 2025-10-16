"""
Comprehensive integration test for the PR changes.
This test demonstrates that all the metadata changes are working properly in an integration context.
"""

from unittest.mock import patch
from singer.messages import SchemaMessage
from base import BaseOrdwayTestCase


class ComprehensivePRTestCase(BaseOrdwayTestCase):
    """
    Comprehensive test case that covers all PR changes:
    1. parent-tap-stream-id addition for substreams
    2. forced-replication-method support
    3. Verification that existing functionality is preserved
    """
    
    START_DATE = "2020-10-16"
    # Test all streams mentioned in the PR
    SELECTED_STREAMS = (
        "customers", "contacts", "payment_methods", "customer_notes", 
        "plans", "charges"
    )

    @classmethod
    def setUpClass(cls):
        with patch("tap_ordway.utils.time", return_value=123):
            super().setUpClass()

    def test_pr_parent_relationships_complete(self):
        """Test all parent-child relationships defined in the PR."""
        
        # These are the exact relationships added in the PR
        expected_relationships = {
            "contacts": "customers",
            "payment_methods": "customers", 
            "customer_notes": "customers",
            "charges": "plans"
        }
        
        schema_messages = list(
            self.tap_executor.output.filter_messages(message_type=SchemaMessage)
        )
        stream_schemas = {msg.stream: msg for msg in schema_messages}
        
        for child_stream, expected_parent in expected_relationships.items():
            with self.subTest(child_stream=child_stream):
                # Verify schema message exists
                self.assertIn(child_stream, stream_schemas,
                            f"Schema message not found for {child_stream}")
                
                # Get metadata
                schema_msg = stream_schemas[child_stream]
                metadata_entries = schema_msg.schema.get('metadata', [])
                
                # Find table-level metadata
                table_metadata = None
                for entry in metadata_entries:
                    if entry.get('breadcrumb') == [] or entry.get('breadcrumb') == ():
                        table_metadata = entry.get('metadata', {})
                        break
                
                self.assertIsNotNone(table_metadata,
                                   f"Table metadata not found for {child_stream}")
                
                # Verify parent relationship
                self.assertIn('parent-tap-stream-id', table_metadata,
                            f"parent-tap-stream-id missing for {child_stream}")
                self.assertEqual(table_metadata['parent-tap-stream-id'], expected_parent,
                               f"Incorrect parent for {child_stream}")

    def test_pr_no_parent_for_parent_streams(self):
        """Test that parent streams do not have parent-tap-stream-id."""
        
        parent_streams = ["customers", "plans"]
        
        schema_messages = list(
            self.tap_executor.output.filter_messages(message_type=SchemaMessage)
        )
        stream_schemas = {msg.stream: msg for msg in schema_messages}
        
        for parent_stream in parent_streams:
            with self.subTest(parent_stream=parent_stream):
                self.assertIn(parent_stream, stream_schemas,
                            f"Schema message not found for {parent_stream}")
                
                schema_msg = stream_schemas[parent_stream]
                metadata_entries = schema_msg.schema.get('metadata', [])
                
                # Find table-level metadata
                table_metadata = None
                for entry in metadata_entries:
                    if entry.get('breadcrumb') == [] or entry.get('breadcrumb') == ():
                        table_metadata = entry.get('metadata', {})
                        break
                
                self.assertIsNotNone(table_metadata,
                                   f"Table metadata not found for {parent_stream}")
                
                # Verify NO parent relationship
                self.assertNotIn('parent-tap-stream-id', table_metadata,
                                f"Parent stream {parent_stream} should not have parent-tap-stream-id")

    def test_pr_forced_replication_method_support(self):
        """Test that forced-replication-method is supported in metadata."""
        
        schema_messages = list(
            self.tap_executor.output.filter_messages(message_type=SchemaMessage)
        )
        
        # Test that forced-replication-method is present in metadata for all streams
        for schema_msg in schema_messages:
            with self.subTest(stream=schema_msg.stream):
                metadata_entries = schema_msg.schema.get('metadata', [])
                
                # Find table-level metadata
                table_metadata = None
                for entry in metadata_entries:
                    if entry.get('breadcrumb') == [] or entry.get('breadcrumb') == ():
                        table_metadata = entry.get('metadata', {})
                        break
                
                self.assertIsNotNone(table_metadata,
                                   f"Table metadata not found for {schema_msg.stream}")
                
                # Verify forced-replication-method is present
                self.assertIn('forced-replication-method', table_metadata,
                            f"forced-replication-method missing for {schema_msg.stream}")

    def test_pr_metadata_structure_integrity(self):
        """Test that metadata structure integrity is maintained after PR changes."""
        
        schema_messages = list(
            self.tap_executor.output.filter_messages(message_type=SchemaMessage)
        )
        
        for schema_msg in schema_messages:
            with self.subTest(stream=schema_msg.stream):
                metadata_entries = schema_msg.schema.get('metadata', [])
                
                # Verify metadata is a list
                self.assertIsInstance(metadata_entries, list,
                                    f"Metadata should be list for {schema_msg.stream}")
                
                # Verify each entry has proper structure
                for entry in metadata_entries:
                    self.assertIn('breadcrumb', entry,
                                f"Breadcrumb missing in metadata entry for {schema_msg.stream}")
                    self.assertIn('metadata', entry,
                                f"Metadata field missing in entry for {schema_msg.stream}")
                
                # Verify table-level metadata exists and has required fields
                table_metadata = None
                for entry in metadata_entries:
                    if entry.get('breadcrumb') == [] or entry.get('breadcrumb') == ():
                        table_metadata = entry.get('metadata', {})
                        break
                
                self.assertIsNotNone(table_metadata,
                                   f"Table metadata missing for {schema_msg.stream}")
                
                # Required fields should still be present
                required_fields = ['table-key-properties', 'inclusion']
                for field in required_fields:
                    self.assertIn(field, table_metadata,
                                f"Required field {field} missing for {schema_msg.stream}")

    def test_pr_all_streams_generate_schemas(self):
        """Test that all selected streams generate schema messages with metadata."""
        
        schema_messages = list(
            self.tap_executor.output.filter_messages(message_type=SchemaMessage)
        )
        
        actual_streams = {msg.stream for msg in schema_messages}
        
        for expected_stream in self.SELECTED_STREAMS:
            self.assertIn(expected_stream, actual_streams,
                         f"Stream {expected_stream} did not generate schema message")

    def test_pr_version_consistency(self):
        """Test that the version change in the PR is reflected."""
        # This tests that the version was updated as part of the PR
        from tap_ordway import __version__
        
        # The PR updated version to 0.4.5
        self.assertEqual(__version__.__version__, "0.4.5",
                        "Version should be updated to 0.4.5 as per PR")
