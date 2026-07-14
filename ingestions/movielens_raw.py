import zipfile
import tempfile
import requests
import boto3
from pathlib import Path
import os

MOVIE_LEN_URL = "https://files.grouplens.org/datasets/movielens/ml-32m.zip"
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localhost:9000")
MINIO_USER = "admin"
MINIO_PASSWORD = "password"
BUCKET_NAME = "warehouse"

# Initializing s3 client
s3_client = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=MINIO_USER,
    aws_secret_access_key=MINIO_PASSWORD,
)


def ingestion():
    print("Initializing raw (bronze) ingestion \n")
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_dir = Path(tmpdir)
        print("Temporary folder created at: {}".format(tmp_dir))

        zip_path = tmp_dir / "movielens.zip"
        print("Downloading ZIP files...")
        response = requests.get(MOVIE_LEN_URL, stream=True)  # Consuming in chunks
        if response.status_code == 200:
            with open(zip_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
        else:
            print("Requests failed. Error:{}".format(requests.status_codes))
            return

        print("Extracting files from the temp directory...")
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(tmp_dir)

        # Tracking down the csv files recursively and sending to MinIO
        print("Sending CSV files to the MinIO persistent volume")
        for csv_file in tmp_dir.rglob("*.csv"):
            s3_destination = "movielens_raw/{}".format(csv_file.name)

            print("Sending: {}".format(csv_file.name))
            s3_client.upload_file(
                Filename=str(csv_file),  # Whole csv path
                Bucket=BUCKET_NAME,
                Key=s3_destination,
            )
            print(
                "File saved in MinIO at: s3://{0}/{1}".format(
                    BUCKET_NAME, s3_destination
                )
            )

        print("\n Process concluded! All files has been successfully saved at MinIO.")


if __name__ == "__main__":
    ingestion()
