# Couchbase Sync Tool

## Description

This is a command-line utility for synchronizing data between two Couchbase clusters. It allows for the one-way transfer of documents from a source bucket to a destination bucket. The tool supports syncing at three levels of granularity: the entire bucket, a specific scope, or a single collection.

<img width="462" height="412" alt="image" src="https://github.com/user-attachments/assets/b23f0839-c864-43f9-b729-e2e089357920" />

## Capabilities

-   **Full Bucket Sync:** Copy all scopes and collections (and their documents) from the source bucket to the destination bucket.
-   **Scope Sync:** Copy all collections (and their documents) within a specific scope from source to destination.
-   **Collection Sync:** Copy all documents from a single source collection to a destination collection.
-   **Interactive CLI:** A simple menu-driven interface to guide the user through the sync process.
-   **Automatic Creation:** The tool will automatically create scopes and collections in the destination bucket if they do not already exist.

## Requirements

-   Python 3.6+
-   Access to both a source and a destination Couchbase cluster.
-   Dependencies can be found in `requirements.txt`.

## Setup and Configuration

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd <repository-directory>
    ```

2.  **Install dependencies:**
    It is recommended to use a virtual environment.
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    ```

3.  **Configure Environment Variables:**
    Create a `.env` file by copying the example file:
    ```bash
    cp .env.example .env
    ```
    Now, edit the `.env` file with the connection details for your source and destination Couchbase clusters.

    ```ini
    # Source Database
    SOURCE_DB_CONNECTION_STRING=couchbase://source-cluster.example.com
    SOURCE_DB_USERNAME=your_username
    SOURCE_DB_PASSWORD=your_password
    SOURCE_DB_BUCKET_NAME=source_bucket

    # Destination Database
    DESTINATION_DB_CONNECTION_STRING=couchbase://destination-cluster.example.com
    DESTINATION_DB_USERNAME=your_username
    DESTINATION_DB_PASSWORD=your_password
    DESTINATION_DB_BUCKET_NAME=destination_bucket
    ```

## How to Run

Once the setup and configuration are complete, you can run the tool with the following command:

```bash
python3 main.py
```

You will be presented with a menu to choose the sync operation you wish to perform. Follow the on-screen prompts to select and confirm your desired operation.

### Running Tests

To run the unit tests for the project, execute:

```bash
python3 tests/test_couchbase_sync.py
```

## Limitations

-   **One-Way Sync:** This tool performs a one-way sync only, from the source to the destination.
-   **Destructive Operation:** The sync operation is destructive. It *replaces* the data in the destination bucket, scope, or collection. It does not perform a merge.
-   **No Metadata Sync:** The tool only syncs documents. It does not sync bucket settings, indexes, user permissions, or document TTL (Time-To-Live).
-   **Performance:** The sync process for very large datasets may be slow as it iterates through all documents in the source.

## Contributing

Contributions are welcome! If you find a bug or have a suggestion for improvement, please open an issue or submit a pull request.
