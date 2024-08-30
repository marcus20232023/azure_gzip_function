import azure.functions as func
import logging
import os
import gzip
from azure.storage.blob import BlobServiceClient, BlobClient

def main(mytimer: func.TimerRequest) -> None:
    logging.info('Python timer trigger function started')

    # Get connection string and container name from environment variables
    connect_str = os.environ['AzureStorageConnectionString']
    container_name = os.environ['ContainerName']
    source_folder = os.environ['SourceFolder']
    destination_folder = os.environ['DestinationFolder']

    # Create the BlobServiceClient object
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    # Get a reference to the container
    container_client = blob_service_client.get_container_client(container_name)

    # List all blobs in the source folder
    blobs = container_client.list_blobs(name_starts_with=source_folder)

    for blob in blobs:
        if blob.name.endswith('.gz'):
            # Get a blob client for the gzipped file
            blob_client = container_client.get_blob_client(blob.name)

            # Download the gzipped content
            gzipped_content = blob_client.download_blob().readall()

            # Unzip the content
            unzipped_content = gzip.decompress(gzipped_content)

            # Create a new blob name for the unzipped file
            unzipped_blob_name = destination_folder + '/' + os.path.splitext(os.path.basename(blob.name))[0]

            # Upload the unzipped content to the new blob
            unzipped_blob_client = container_client.get_blob_client(unzipped_blob_name)
            unzipped_blob_client.upload_blob(unzipped_content, overwrite=True)

            logging.info(f'Unzipped and uploaded: {unzipped_blob_name}')

    logging.info('Python timer trigger function completed')