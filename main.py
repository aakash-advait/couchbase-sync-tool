import logging
import sys

from config import SOURCE_DB_CONFIG, DESTINATION_DB_CONFIG
from couchbase_sync import CouchbaseSync, connect_to_couchbase

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_choice(options):
    for i, option in enumerate(options, 1):
        print(f"  {i}. {option}")

    while True:
        try:
            choice = input("Enter your choice: ")
            choice_index = int(choice) - 1
            if 0 <= choice_index < len(options):
                return options[choice_index]
            else:
                print("Invalid choice. Please try again.")
        except ValueError:
            print("Invalid input. Please enter a number.")


def main():
    source_cluster = connect_to_couchbase(SOURCE_DB_CONFIG)
    dest_cluster = connect_to_couchbase(DESTINATION_DB_CONFIG)

    if not source_cluster or not dest_cluster:
        sys.exit("Failed to connect to one or more Couchbase clusters. Exiting.")

    sync_manager = CouchbaseSync(source_cluster, dest_cluster,
                                 SOURCE_DB_CONFIG["bucket_name"],
                                 DESTINATION_DB_CONFIG["bucket_name"])

    print("\nCouchbase Sync Tool")
    print("===================")
    print("Select an option:")
    print("  1. Copy (replace) entire database (bucket)")
    print("  2. Copy (replace) an entire scope")
    print("  3. Copy (replace) an entire collection")
    print("  4. Exit")

    while True:
        choice = input("Enter your choice (1-4): ")
        if choice == '1':
            confirm = input(f"Are you sure you want to replace the entire bucket '{DESTINATION_DB_CONFIG['bucket_name']}'? (yes/no): ")
            if confirm.lower() == 'yes':
                sync_manager.sync_database()
            break
        elif choice == '2':
            scopes = sync_manager.get_scopes()
            scope_names = [s.name for s in scopes if not s.name.startswith('_')]
            if not scope_names:
                print("No scopes found in source bucket.")
                break

            print("\nSelect a scope to sync:")
            selected_scope = get_choice(scope_names)

            confirm = input(
                f"Are you sure you want to replace the scope '{selected_scope}' in bucket '{DESTINATION_DB_CONFIG['bucket_name']}'? (yes/no): ")
            if confirm.lower() == 'yes':
                sync_manager.sync_scope(selected_scope)
            break
        elif choice == '3':
            scopes = sync_manager.get_scopes()
            scope_names = [s.name for s in scopes if not s.name.startswith('_')]
            if not scope_names:
                print("No scopes found in source bucket.")
                break

            print("\nSelect a scope:")
            selected_scope_name = get_choice(scope_names)

            collections = sync_manager.get_collections_for_scope(selected_scope_name)
            collection_names = [c.name for c in collections if not c.name.startswith('_')]
            if not collection_names:
                print(f"No collections found in scope '{selected_scope_name}'.")
                break

            print("\nSelect a collection to sync:")
            selected_collection = get_choice(collection_names)

            confirm = input(
                f"Are you sure you want to replace the collection '{selected_scope_name}.{selected_collection}' in bucket '{DESTINATION_DB_CONFIG['bucket_name']}'? (yes/no): ")
            if confirm.lower() == 'yes':
                sync_manager.sync_collection(selected_scope_name, selected_collection)
            break
        elif choice == '4':
            print("Exiting.")
            break
        else:
            print("Invalid choice. Please enter a number between 1 and 4.")

    if source_cluster:
        source_cluster.disconnect()
    if dest_cluster:
        dest_cluster.disconnect()


if __name__ == "__main__":
    main()
