"""Unit tests for property.py metadata generation including parent-tap-stream-id support."""

import unittest
from tap_ordway.property import get_stream_metadata, get_key_properties, get_replication_method


class TestPropertyMetadata(unittest.TestCase):
    """Test cases for property.py metadata generation functions."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_schema = {
            "type": "object",
            "properties": {
                "id": {"type": "string"},
                "name": {"type": "string"},
                "updated_date": {"type": "string"}
            }
        }

    def test_get_stream_metadata_contacts_has_parent(self):
        """Test that contacts stream metadata includes parent-tap-stream-id."""
        metadata = get_stream_metadata('contacts', self.mock_schema)
        
        # Find table-level metadata
        table_metadata = None
        for entry in metadata:
            if entry.get('breadcrumb') == () or entry.get('breadcrumb') == []:
                table_metadata = entry.get('metadata', {})
                break
        
        self.assertIsNotNone(table_metadata, "Table-level metadata should exist")
        self.assertIn('parent-tap-stream-id', table_metadata, 
                     "Contacts should have parent-tap-stream-id")
        self.assertEqual(table_metadata['parent-tap-stream-id'], 'customers',
                        "Contacts parent should be customers")

    def test_get_stream_metadata_customers_no_parent(self):
        """Test that customers stream metadata does not include parent-tap-stream-id."""
        metadata = get_stream_metadata('customers', self.mock_schema)
        
        # Find table-level metadata
        table_metadata = None
        for entry in metadata:
            if entry.get('breadcrumb') == () or entry.get('breadcrumb') == []:
                table_metadata = entry.get('metadata', {})
                break
        
        self.assertIsNotNone(table_metadata, "Table-level metadata should exist")
        self.assertNotIn('parent-tap-stream-id', table_metadata,
                        "Customers should not have parent-tap-stream-id")

    def test_get_stream_metadata_payment_methods_has_parent(self):
        """Test that payment_methods stream metadata includes parent-tap-stream-id."""
        metadata = get_stream_metadata('payment_methods', self.mock_schema)
        
        # Find table-level metadata
        table_metadata = None
        for entry in metadata:
            if entry.get('breadcrumb') == () or entry.get('breadcrumb') == []:
                table_metadata = entry.get('metadata', {})
                break
        
        self.assertIsNotNone(table_metadata, "Table-level metadata should exist")
        self.assertIn('parent-tap-stream-id', table_metadata,
                     "Payment methods should have parent-tap-stream-id")
        self.assertEqual(table_metadata['parent-tap-stream-id'], 'customers',
                        "Payment methods parent should be customers")

    def test_get_stream_metadata_charges_has_parent(self):
        """Test that charges stream metadata includes parent-tap-stream-id."""
        metadata = get_stream_metadata('charges', self.mock_schema)
        
        # Find table-level metadata
        table_metadata = None
        for entry in metadata:
            if entry.get('breadcrumb') == () or entry.get('breadcrumb') == []:
                table_metadata = entry.get('metadata', {})
                break
        
        self.assertIsNotNone(table_metadata, "Table-level metadata should exist")
        self.assertIn('parent-tap-stream-id', table_metadata,
                     "Charges should have parent-tap-stream-id")
        self.assertEqual(table_metadata['parent-tap-stream-id'], 'plans',
                        "Charges parent should be plans")

    def test_get_stream_metadata_plans_no_parent(self):
        """Test that plans stream metadata does not include parent-tap-stream-id."""
        metadata = get_stream_metadata('plans', self.mock_schema)
        
        # Find table-level metadata
        table_metadata = None
        for entry in metadata:
            if entry.get('breadcrumb') == () or entry.get('breadcrumb') == []:
                table_metadata = entry.get('metadata', {})
                break
        
        self.assertIsNotNone(table_metadata, "Table-level metadata should exist")
        self.assertNotIn('parent-tap-stream-id', table_metadata,
                        "Plans should not have parent-tap-stream-id")

    def test_forced_replication_method_in_metadata(self):
        """Test that forced-replication-method appears in metadata."""
        metadata = get_stream_metadata('customers', self.mock_schema)
        
        # Find table-level metadata
        table_metadata = None
        for entry in metadata:
            if entry.get('breadcrumb') == () or entry.get('breadcrumb') == []:
                table_metadata = entry.get('metadata', {})
                break
        
        self.assertIsNotNone(table_metadata, "Table-level metadata should exist")
        self.assertIn('forced-replication-method', table_metadata,
                     "Metadata should include forced-replication-method field")

    def test_get_key_properties_function(self):
        """Test that get_key_properties returns correct values."""
        contacts_keys = get_key_properties('contacts')
        customers_keys = get_key_properties('customers')
        
        self.assertIsNotNone(contacts_keys, "Contacts should have key properties")
        self.assertIsNotNone(customers_keys, "Customers should have key properties")
        self.assertIsInstance(contacts_keys, list, "Key properties should be a list")
        self.assertIsInstance(customers_keys, list, "Key properties should be a list")

    def test_get_replication_method_function(self):
        """Test that get_replication_method returns correct values."""
        contacts_method = get_replication_method('contacts')
        customers_method = get_replication_method('customers')
        
        self.assertIsNotNone(contacts_method, "Contacts should have replication method")
        self.assertIsNotNone(customers_method, "Customers should have replication method")
        self.assertIn(contacts_method, ['FULL_TABLE', 'INCREMENTAL'], 
                     "Should be valid replication method")
        self.assertIn(customers_method, ['FULL_TABLE', 'INCREMENTAL'], 
                     "Should be valid replication method")
