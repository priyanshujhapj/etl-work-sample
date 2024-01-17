import configparser, os
from azure.storage.blob import BlobServiceClient

config = configparser.ConfigParser()
config.read('config.ini')
con_str = config['main']['connection_string']

container_name  = "dataflowmonitoringproject"

if __name__ == "__main__":
    # list of files to download
    files = ['dataflow_api_request.py', 'aadservice.py', 'list_of_blobs.py']
    blob_service_client = BlobServiceClient.from_connection_string(conn_str)

    for fil in files:
        # local_file_path = "dataflow/config.ini"
        local_file_path = "dataflow/{0}".format(fil)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_path)


        # download_file_path = "config.ini"
        download_file_path = str(fil)
        print('\nDownloading blob to \n\t' + download_file_path)


        with open(download_file_path, 'wb') as download_file:
            download_file.write(blob_client.download_blob().readall())
