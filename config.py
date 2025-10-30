import os
from dotenv import load_dotenv

load_dotenv()

# SOURCE DATABASE CONFIGURATION
# Credentials are loaded from the .env file.
SOURCE_DB_CONFIG = {
    "connection_string": os.getenv("SOURCE_DB_CONNECTION_STRING"),
    "username": os.getenv("SOURCE_DB_USERNAME"),
    "password": os.getenv("SOURCE_DB_PASSWORD"),
    "bucket_name": os.getenv("SOURCE_DB_BUCKET_NAME")
}

# DESTINATION DATABASE CONFIGURATION
# Credentials are loaded from the .env file.
DESTINATION_DB_CONFIG = {
    "connection_string": os.getenv("DESTINATION_DB_CONNECTION_STRING"),
    "username": os.getenv("DESTINATION_DB_USERNAME"),
    "password": os.getenv("DESTINATION_DB_PASSWORD"),
    "bucket_name": os.getenv("DESTINATION_DB_BUCKET_NAME")
}
