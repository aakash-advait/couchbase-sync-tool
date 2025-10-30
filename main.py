import logging
import sys

from config import SOURCE_DB_CONFIG, DESTINATION_DB_CONFIG
from couchbase_sync import CouchbaseSync, connect_to_couchbase

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# New constants for navigation
GO_BACK_PREVIOUS = "Go back to previous menu"
GO_BACK_MAIN = "Go back to main menu"


def get_choice(options, nav_options=None):
    if nav_options is None:
        nav_options = []

    # Print nav options
    for i, option in enumerate(nav_options, start=-len(nav_options)):
        print(f"  {i}. {option}")

    # Print regular options
    for i, option in enumerate(options, 1):
        print(f"  {i}. {option}")

    while True:
        try:
            choice_str = input("Enter your choice: ")
            choice = int(choice_str)

            if choice == 0:
                print("Invalid choice. Please try again.")
                continue

            if choice < 0:
                # Negative choices for navigation
                if -len(nav_options) <= choice <= -1:
                    return nav_options[choice]
            else:
                # Positive choices for regular options
                choice_index = choice - 1
                if 0 <= choice_index < len(options):
                    return options[choice_index]

            print("Invalid choice. Please try again.")
        except ValueError:
            print("Invalid input. Please enter a number.")


def sync_scope_menu(sync_manager):
    while True:
        scopes = sync_manager.get_scopes()
        scope_names = [s.name for s in scopes if not s.name.startswith('_')]
        if not scope_names:
            print("No scopes found in source bucket.")
            return

        print("\nSelect a scope to sync:")
        selected_scope = get_choice(scope_names, [GO_BACK_MAIN])

        if selected_scope == GO_BACK_MAIN:
            return

        confirm = input(
            f"Are you sure you want to replace the scope '{selected_scope}' in bucket '{DESTINATION_DB_CONFIG['bucket_name']}'? (yes/no): ")
        if confirm.lower() == 'yes':
            if sync_manager.sync_scope(selected_scope):
                logging.info(f"Scope '{selected_scope}' sync completed successfully.")
            else:
                logging.error(f"Scope '{selected_scope}' sync completed with errors. Check logs for details.")


def sync_collection_menu(sync_manager):
    while True:
        scopes = sync_manager.get_scopes()
        scope_names = [s.name for s in scopes if not s.name.startswith('_')]
        if not scope_names:
            print("No scopes found in source bucket.")
            return

        print("\nSelect a scope:")
        selected_scope_name = get_choice(scope_names, [GO_BACK_MAIN])

        if selected_scope_name == GO_BACK_MAIN:
            return

        while True:
            collections = sync_manager.get_collections_for_scope(selected_scope_name)
            collection_names = [c.name for c in collections if not c.name.startswith('_')]
            if not collection_names:
                print(f"No collections found in scope '{selected_scope_name}'.")
                break

            print("\nSelect a collection to sync:")
            selected_collection = get_choice(collection_names, [GO_BACK_MAIN, GO_BACK_PREVIOUS])

            if selected_collection == GO_BACK_MAIN:
                return
            if selected_collection == GO_BACK_PREVIOUS:
                break

            confirm = input(
                f"Are you sure you want to replace the collection '{selected_scope_name}.{selected_collection}' in bucket '{DESTINATION_DB_CONFIG['bucket_name']}'? (yes/no): ")
            if confirm.lower() == 'yes':
                if sync_manager.sync_collection(selected_scope_name, selected_collection):
                    logging.info(
                        f"Collection '{selected_scope_name}.{selected_collection}' sync completed successfully.")
                else:
                    logging.error(
                        f"Collection '{selected_scope_name}.{selected_collection}' sync completed with errors. Check logs for details.")


def main():
    source_cluster = connect_to_couchbase(SOURCE_DB_CONFIG)
    dest_cluster = connect_to_couchbase(DESTINATION_DB_CONFIG)

    if not source_cluster or not dest_cluster:
        sys.exit("Failed to connect to one or more Couchbase clusters. Exiting.")

    sync_manager = CouchbaseSync(source_cluster, dest_cluster,
                                 SOURCE_DB_CONFIG["bucket_name"],
                                 DESTINATION_DB_CONFIG["bucket_name"])

    while True:
        print("\nCouchbase Sync Tool")
        print("===================")
        print("Select an option:")
        print("  1. Copy (replace) entire database (bucket)")
        print("  2. Copy (replace) an entire scope")
        print("  3. Copy (replace) an entire collection")
        print("  4. Exit")

        choice = input("Enter your choice (1-4): ")
        if choice == '1':
            confirm = input(f"Are you sure you want to replace the entire bucket '{DESTINATION_DB_CONFIG['bucket_name']}'? (yes/no): ")
            if confirm.lower() == 'yes':
                if sync_manager.sync_database():
                    logging.info("Database sync completed successfully.")
                else:
                    logging.error("Database sync completed with errors. Check logs for details.")
        elif choice == '2':
            sync_scope_menu(sync_manager)
        elif choice == '3':
            sync_collection_menu(sync_manager)
        elif choice == '4':
            print("Exiting.")
            break
        else:
            print("Invalid choice. Please enter a number between 1 and 4.")



if __name__ == "__main__":
    main()
