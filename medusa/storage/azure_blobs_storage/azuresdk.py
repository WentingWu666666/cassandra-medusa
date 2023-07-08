import io
import json
import os

from azure.storage.blob import BlobServiceClient
from retrying import retry


class AzureBlobService(object):

    def __init__(self, storage):
        self._config = storage.config
        self.storage = storage

    def __enter__(self):
        with io.open(os.path.expanduser(self._config.key_file), 'r', encoding='utf-8') as json_fi:
            credentials = json.load(json_fi)

        if 'connection_string' in credentials:
            self._blob_service_client = BlobServiceClient.from_connection_string(credentials['connection_string'])
        else:
            if 'host' in credentials:
                url = "https://{}".format(
                    credentials['host']
                )
            else:
                url = "https://{}.blob.core.windows.net".format(
                    credentials['storage_account']
                )
            self._blob_service_client = BlobServiceClient(account_url=url, credential=credentials['key'])

        return self

    def upload_blob(self, *, bucket_name, src, dest):
        self._upload_blob(bucket_name=bucket_name, src=src, dest=dest)
        return self._get_blob(dest)

    @retry(stop_max_attempt_number=5, wait_fixed=5000)
    def _upload_blob(self, *, bucket_name, src, dest):
        container_client = self._blob_service_client.get_container_client(bucket_name)
        blob_client = container_client.get_blob_client(dest)
        with open(src, "rb") as data:
            blob_client.upload_blob(data, blob_type="BlockBlob", overwrite=True)

    def download_blob(self, *, bucket_name, src, dest):
        dest_path = os.path.join(str(dest), str(src).split("/")[-1])
        self._download_file(bucket_name, src, dest_path)

    @retry(stop_max_attempt_number=5, wait_fixed=5000)
    def _download_blob(self, bucket_name, src, dest):
        container_client = self._blob_service_client.get_container_client(bucket_name)
        blob_client = container_client.get_blob_client(str(src))

        with open(dest, "wb") as data:
            stream = blob_client.download_blob()
            stream.readinto(data)

    @retry(stop_max_attempt_number=5, wait_fixed=5000)
    def _download_file(self, bucket_name, src, dest):
        container_client = self._blob_service_client.get_container_client(bucket_name)
        blob_client = container_client.get_blob_client(str(src))

        with open(dest, "wb") as data:
            stream = blob_client.download_blob()
            stream.readinto(data)

    @retry(stop_max_attempt_number=10, wait_fixed=1000)
    def _get_blob(self, blob_name):
        # This needs to be retried as AZ is eventually consistent
        obj = self.storage.get_blob(blob_name)
        if obj is None:
            raise IOError("Failed to find uploaded object {} in Azure".format(blob_name))
        return obj
