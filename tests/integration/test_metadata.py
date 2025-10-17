"""Integration tests for metadata functionality added in PR #27"""
import unittest
from singer import metadata
from tap_ordway import discover


class TestMetadataChanges(unittest.TestCase):
    """Test metadata functionality including parent-tap-stream-id and replication-method"""

    def test_substream_parent_metadata(self):
        """Test that substreams have parent-tap-stream-id metadata"""
        catalog = discover()
        
        # Expected parent-child relationships based on the PR changes
        expected_parents = {
            "contacts": "customers",
            "payment_methods": "customers", 
            "customer_notes": "customers",
            "charges": "plans"
        }
        
        for entry in catalog.streams:
            stream_id = entry.tap_stream_id
            
            if stream_id in expected_parents:
                # Convert metadata to map for easier access
                mdata = metadata.to_map(entry.metadata)
                
                # Check that parent-tap-stream-id exists in metadata
                self.assertIn("parent-tap-stream-id", mdata.get((), {}),
                             f"Stream '{stream_id}' should have parent-tap-stream-id metadata")
                
                # Check that the parent is correct
                actual_parent = mdata[()]["parent-tap-stream-id"]
                expected_parent = expected_parents[stream_id]
                self.assertEqual(actual_parent, expected_parent,
                               f"Stream '{stream_id}' should have parent '{expected_parent}', got '{actual_parent}'")

    def test_parent_streams_no_parent_metadata(self):
        """Test that parent streams do not have parent-tap-stream-id metadata"""
        catalog = discover()
        
        # These are parent streams that should not have parent metadata
        parent_streams = ["customers", "plans"]
        
        for entry in catalog.streams:
            stream_id = entry.tap_stream_id
            
            if stream_id in parent_streams:
                # Convert metadata to map for easier access
                mdata = metadata.to_map(entry.metadata)
                
                # Check that parent-tap-stream-id does not exist
                self.assertNotIn("parent-tap-stream-id", mdata.get((), {}),
                                f"Parent stream '{stream_id}' should not have parent-tap-stream-id metadata")

    def test_replication_method_in_metadata(self):
        """Test that replication method is properly included in metadata"""
        catalog = discover()
        
        # Expected replication methods based on stream definitions
        expected_replication_methods = {
            "customers": "FULL_TABLE",
            "contacts": "FULL_TABLE",
            "payment_methods": "FULL_TABLE",
            "customer_notes": "FULL_TABLE",
            "plans": "FULL_TABLE",
            "charges": "FULL_TABLE"
        }
        
        for entry in catalog.streams:
            stream_id = entry.tap_stream_id
            
            if stream_id in expected_replication_methods:
                # Check that replication method is set correctly in the catalog entry
                actual_method = entry.replication_method
                expected_method = expected_replication_methods[stream_id]
                self.assertEqual(actual_method, expected_method,
                               f"Stream '{stream_id}' should have replication method '{expected_method}', got '{actual_method}'")
                
                # Also check that it's in the metadata as forced-replication-method (new feature)
                mdata = metadata.to_map(entry.metadata)
                metadata_method = mdata.get((), {}).get("forced-replication-method")
                self.assertEqual(metadata_method, expected_method,
                               f"Stream '{stream_id}' metadata should have forced-replication-method '{expected_method}', got '{metadata_method}'")

    def test_catalog_structure_integrity(self):
        """Test that catalog structure is maintained after metadata changes"""
        catalog = discover()
        
        # Ensure all expected streams are present
        stream_ids = {entry.tap_stream_id for entry in catalog.streams}
        
        # These are the streams we know should exist based on the codebase
        expected_streams = {
            "customers", "contacts", "payment_methods", "customer_notes",
            "plans", "charges", "invoices", "payments", "refunds",
            "billing_runs", "billing_schedules", "chart_of_accounts",
            "coupons", "credits", "debit_memo", "journal_entries",
            "orders", "payment_runs", "products", "revenue_rules",
            "revenue_schedules", "statements", "subscriptions", "usages", "webhooks"
        }
        
        # Check that all expected streams are present
        for expected_stream in expected_streams:
            self.assertIn(expected_stream, stream_ids,
                         f"Expected stream '{expected_stream}' not found in catalog")
        
        # Ensure each catalog entry has required fields
        for entry in catalog.streams:
            self.assertIsNotNone(entry.tap_stream_id, "Stream should have tap_stream_id")
            self.assertIsNotNone(entry.schema, "Stream should have schema")
            self.assertIsNotNone(entry.metadata, "Stream should have metadata")
            self.assertIsInstance(entry.metadata, list, "Metadata should be a list")

    def test_metadata_format_consistency(self):
        """Test that metadata follows the expected format"""
        catalog = discover()
        
        for entry in catalog.streams:
            # Metadata should be a list of dictionaries
            self.assertIsInstance(entry.metadata, list,
                                f"Metadata for '{entry.tap_stream_id}' should be a list")
            
            # Each metadata entry should have breadcrumb and metadata keys
            for meta_entry in entry.metadata:
                self.assertIsInstance(meta_entry, dict,
                                    f"Each metadata entry for '{entry.tap_stream_id}' should be a dict")
                self.assertIn("breadcrumb", meta_entry,
                            f"Metadata entry for '{entry.tap_stream_id}' should have 'breadcrumb' key")
                self.assertIn("metadata", meta_entry,
                            f"Metadata entry for '{entry.tap_stream_id}' should have 'metadata' key")

    def test_forced_replication_method_feature(self):
        """Test the new forced-replication-method feature added in PR #27"""
        catalog = discover()
        
        # All streams should have forced-replication-method in their metadata
        for entry in catalog.streams:
            mdata = metadata.to_map(entry.metadata)
            stream_metadata = mdata.get((), {})
            
            # Check that forced-replication-method exists
            self.assertIn("forced-replication-method", stream_metadata,
                         f"Stream '{entry.tap_stream_id}' should have forced-replication-method in metadata")
            
            # Check that it matches the stream's replication method
            forced_method = stream_metadata["forced-replication-method"]
            self.assertEqual(forced_method, entry.replication_method,
                           f"Stream '{entry.tap_stream_id}' forced-replication-method should match replication_method")

    def test_complete_parent_child_relationships(self):
        """Test all parent-child relationships are correctly represented"""
        catalog = discover()
        
        # Get all stream definitions
        stream_lookup = {entry.tap_stream_id: entry for entry in catalog.streams}
        
        # Verify that all expected relationships exist
        parent_child_pairs = [
            ("customers", "contacts"),
            ("customers", "payment_methods"),
            ("customers", "customer_notes"),
            ("plans", "charges")
        ]
        
        for parent_id, child_id in parent_child_pairs:
            # Ensure both parent and child exist
            self.assertIn(parent_id, stream_lookup, f"Parent stream '{parent_id}' should exist")
            self.assertIn(child_id, stream_lookup, f"Child stream '{child_id}' should exist")
            
            # Check child has parent metadata
            child_entry = stream_lookup[child_id]
            mdata = metadata.to_map(child_entry.metadata)
            parent_in_metadata = mdata.get((), {}).get("parent-tap-stream-id")
            
            self.assertEqual(parent_in_metadata, parent_id,
                           f"Child stream '{child_id}' should have parent '{parent_id}' in metadata")
            
            # Check parent does not have parent metadata
            parent_entry = stream_lookup[parent_id]
            parent_mdata = metadata.to_map(parent_entry.metadata)
            parent_has_parent = "parent-tap-stream-id" in parent_mdata.get((), {})
            
            self.assertFalse(parent_has_parent,
                           f"Parent stream '{parent_id}' should not have parent-tap-stream-id metadata")
