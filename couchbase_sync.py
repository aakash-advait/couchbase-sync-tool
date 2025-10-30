import logging
import time
from datetime import timedelta

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster, ClusterOptions
from couchbase.exceptions import CouchbaseException

log = logging.getLogger(__name__)


class CouchbaseSync:
    def __init__(self, source_cluster, dest_cluster, source_bucket_name, dest_bucket_name):
        self.source_cluster = source_cluster
        self.dest_cluster = dest_cluster
        self.source_bucket = source_cluster.bucket(source_bucket_name)
        self.dest_bucket = dest_cluster.bucket(dest_bucket_name)
        self.source_bucket_name = source_bucket_name
        self.dest_bucket_name = dest_bucket_name
        self.source_coll_manager = self.source_bucket.collections()
        self.dest_coll_manager = self.dest_bucket.collections()

    def get_scopes(self):
        try:
            return self.source_coll_manager.get_all_scopes()
        except CouchbaseException as e:
            log.error(f"Error getting scopes: {e}")
            return []

    def get_collections_for_scope(self, scope_name):
        try:
            scopes = self.source_coll_manager.get_all_scopes()
            for scope in scopes:
                if scope.name == scope_name:
                    return scope.collections
            return []
        except CouchbaseException as e:
            log.error(f"Error getting collections for scope {scope_name}: {e}")
            return []

    def _ensure_collection_exists(self, scope_name, collection_name):
        try:
            all_scopes = self.dest_coll_manager.get_all_scopes()
            found_scope = next((s for s in all_scopes if s.name == scope_name), None)

            if not found_scope:
                log.info(f"Creating scope '{scope_name}' on destination.")
                self.dest_coll_manager.create_scope(scope_name)
                time.sleep(2)  # Wait for scope to be available
                log.info(f"Creating collection '{scope_name}.{collection_name}' on destination.")
                self.dest_coll_manager.create_collection(scope_name, collection_name)
                time.sleep(2)  # Wait for collection to be available
            else:
                coll_names = [c.name for c in found_scope.collections]
                if collection_name not in coll_names:
                    log.info(f"Creating collection '{scope_name}.{collection_name}' on destination.")
                    self.dest_coll_manager.create_collection(scope_name, collection_name)
                    time.sleep(2)  # Wait for collection to be available
        except CouchbaseException as e:
            log.error(f"Failed to create scope/collection '{scope_name}.{collection_name}': {e}")
            raise

    def sync_collection(self, scope_name, collection_name):
        log.info(f"Starting sync for collection '{scope_name}.{collection_name}'")
        try:
            self._ensure_collection_exists(scope_name, collection_name)

            dest_collection = self.dest_bucket.scope(scope_name).collection(collection_name)

            query = (f"SELECT RAW {{'id': META(t).id, 'doc': t}} "
                     f"FROM `{self.source_bucket_name}`.`{scope_name}`.`{collection_name}` AS t")
            result = self.source_cluster.query(query)

            doc_count = 0
            for row in result.rows():
                doc_id = row['id']
                doc_content = row['doc']
                dest_collection.upsert(doc_id, doc_content)
                doc_count += 1

            log.info(f"Successfully synced {doc_count} documents in collection '{scope_name}.{collection_name}'")
            return True

        except CouchbaseException as e:
            log.error(f"An error occurred during collection sync: {e}")
            return False

    def sync_scope(self, scope_name):
        log.info(f"Starting sync for scope '{scope_name}'")
        collections = self.get_collections_for_scope(scope_name)
        all_successful = True
        for collection in collections:
            if collection.name.startswith('_'):
                continue
            if not self.sync_collection(scope_name, collection.name):
                all_successful = False
        log.info(f"Finished sync for scope '{scope_name}'")
        return all_successful

    def sync_database(self):
        log.info(f"Starting sync for bucket '{self.source_bucket_name}'")
        scopes = self.get_scopes()
        all_successful = True
        for scope in scopes:
            if scope.name.startswith('_'):
                continue
            if not self.sync_scope(scope.name):
                all_successful = False
        log.info(f"Finished sync for bucket '{self.source_bucket_name}'")
        return all_successful


def connect_to_couchbase(config):
    try:
        auth = PasswordAuthenticator(config["username"], config["password"])
        options = ClusterOptions(auth, query_timeout=timedelta(seconds=300), kv_timeout=timedelta(seconds=30))
        cluster = Cluster(config["connection_string"], options)
        cluster.wait_until_ready(timedelta(seconds=5))
        log.info(f"Connected to Couchbase cluster at {config['connection_string']}")
        return cluster
    except Exception as e:
        log.error(f"Could not connect to Couchbase at {config['connection_string']}: {e}")
        return None
