import unittest
from unittest.mock import MagicMock, call, patch

from couchbase_sync import CouchbaseSync


# Mock CollectionSpec and ScopeSpec as they are not easily mockable classes
class MockCollectionSpec:
    def __init__(self, name):
        self.name = name


class MockScopeSpec:
    def __init__(self, name, collections):
        self.name = name
        self.collections = [MockCollectionSpec(c) for c in collections]


class TestCouchbaseSync(unittest.TestCase):
    def setUp(self):
        self.mock_source_cluster = MagicMock()
        self.mock_dest_cluster = MagicMock()
        self.source_bucket_name = "source_bucket"
        self.dest_bucket_name = "dest_bucket"

        self.mock_source_bucket = self.mock_source_cluster.bucket.return_value
        self.mock_dest_bucket = self.mock_dest_cluster.bucket.return_value

        self.mock_source_coll_manager = self.mock_source_bucket.collections.return_value
        self.mock_dest_coll_manager = self.mock_dest_bucket.collections.return_value

        self.sync_manager = CouchbaseSync(
            self.mock_source_cluster,
            self.mock_dest_cluster,
            self.source_bucket_name,
            self.dest_bucket_name
        )
        self.sync_manager.source_bucket_name = self.source_bucket_name

    def test_get_scopes(self):
        mock_scopes = [
            MockScopeSpec("_default", ["_default"]),
            MockScopeSpec("users", ["profiles", "posts"])
        ]
        self.mock_source_coll_manager.get_all_scopes.return_value = mock_scopes

        scopes = self.sync_manager.get_scopes()
        self.assertEqual(len(scopes), 2)
        self.assertEqual(scopes[1].name, "users")

    def test_get_collections_for_scope(self):
        mock_scopes = [
            MockScopeSpec("users", ["profiles", "posts"])
        ]
        self.mock_source_coll_manager.get_all_scopes.return_value = mock_scopes

        collections = self.sync_manager.get_collections_for_scope("users")
        self.assertEqual(len(collections), 2)
        self.assertEqual(collections[0].name, "profiles")
        self.assertEqual(collections[1].name, "posts")

    @patch('time.sleep', return_value=None)
    def test_ensure_collection_exists_creates_scope_and_collection(self, mock_sleep):
        self.mock_dest_coll_manager.get_all_scopes.return_value = []
        self.sync_manager._ensure_collection_exists("new_scope", "new_coll")
        self.mock_dest_coll_manager.create_scope.assert_called_once_with("new_scope")
        self.mock_dest_coll_manager.create_collection.assert_called_once_with("new_scope", "new_coll")

    @patch('couchbase_sync.CouchbaseSync._ensure_collection_exists')
    def test_sync_collection(self, mock_ensure_collection):
        mock_dest_collection = MagicMock()
        self.mock_dest_bucket.scope.return_value.collection.return_value = mock_dest_collection

        mock_query_result = MagicMock()
        mock_query_result.rows.return_value = [
            {'id': 'user1', 'doc': {'name': 'Alice'}},
            {'id': 'user2', 'doc': {'name': 'Bob'}}
        ]
        self.mock_source_cluster.query.return_value = mock_query_result

        self.sync_manager.sync_collection("users", "profiles")

        mock_ensure_collection.assert_called_once_with("users", "profiles")

        expected_query = (f"SELECT RAW {{'id': META(t).id, 'doc': t}} "
                          f"FROM `{self.source_bucket_name}`.`users`.`profiles` AS t")
        self.mock_source_cluster.query.assert_called_once_with(expected_query)

        calls = [
            call('user1', {'name': 'Alice'}),
            call('user2', {'name': 'Bob'})
        ]
        mock_dest_collection.upsert.assert_has_calls(calls, any_order=True)
        self.assertEqual(mock_dest_collection.upsert.call_count, 2)

    def test_sync_scope(self):
        mock_collections = [MockCollectionSpec("profiles"), MockCollectionSpec("posts")]
        with patch.object(self.sync_manager, 'get_collections_for_scope', return_value=mock_collections) as mock_get_colls, \
             patch.object(self.sync_manager, 'sync_collection') as mock_sync_collection:
            self.sync_manager.sync_scope("users")
            mock_get_colls.assert_called_once_with("users")
            calls = [
                call("users", "profiles"),
                call("users", "posts")
            ]
            mock_sync_collection.assert_has_calls(calls, any_order=True)

    def test_sync_database(self):
        mock_scopes = [
            MockScopeSpec("_default", ["_default"]),
            MockScopeSpec("users", ["profiles"]),
            MockScopeSpec("inventory", ["items"])
        ]
        self.mock_source_coll_manager.get_all_scopes.return_value = mock_scopes

        with patch.object(self.sync_manager, 'sync_scope') as mock_sync_scope:
            self.sync_manager.sync_database()
            calls = [
                call("users"),
                call("inventory")
            ]
            mock_sync_scope.assert_has_calls(calls, any_order=True)
            self.assertEqual(mock_sync_scope.call_count, 2)


if __name__ == '__main__':
    unittest.main()
