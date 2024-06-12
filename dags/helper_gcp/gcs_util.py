import pandas as pd
from google.cloud import storage
from io import BytesIO

class GoogleCloudStorage:
    def __init__(self, bucket_name):
        self.keyfile_path = './dags/credentials/gcs.json'
        self.bucket_name = bucket_name
        self.client = None

    def authenticate(self):
        """Authenticates the client using the JSON key file."""
        if not self.client:
            self.client = storage.Client.from_service_account_json(self.keyfile_path)

    def list_objects(self):
        """Lists objects in the specified bucket."""
        self.authenticate()
        bucket = self.client.get_bucket(self.bucket_name)
        blobs = bucket.list_blobs()
        for blob in blobs:
            print(blob.name)


    def upload_file(self, local_file_path, destination_blob_name):
        """Uploads a file to the specified bucket."""
        self.authenticate()
        bucket = self.client.get_bucket(self.bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(local_file_path)

    def download_to_dataframe(self, source_blob_name):
        """
        Downloads a CSV file from the specified bucket and returns it as a pandas DataFrame.
        """
        try:
            # Authenticate and download blob
            self.authenticate()
            bucket = self.client.get_bucket(self.bucket_name)
            blob = bucket.blob(source_blob_name)
            csv_data = blob.download_as_bytes()
            
            # Convert bytes data to a DataFrame
            df = pd.read_csv(BytesIO(csv_data))
            return df
        except Exception as e:
            raise Exception(f"Error downloading CSV as DataFrame: {e}")

    
    def save_dataframe_to_csv(self, object_name, dataframe):
            """
            Saves a pandas DataFrame to a CSV file and uploads it to the specified Google Cloud Storage bucket.
            """
            try:
                # Convert DataFrame to CSV
                csv_buffer = BytesIO()
                dataframe.to_csv(csv_buffer, index=False)
                csv_buffer.seek(0)

                # Authenticate and upload to GCS
                self.authenticate()
                bucket = self.client.get_bucket(self.bucket_name)
                blob = bucket.blob(object_name)
                blob.upload_from_file(csv_buffer, content_type='text/csv')
            except Exception as e:
                raise Exception(f"Error saving DataFrame to CSV and uploading: {e}")