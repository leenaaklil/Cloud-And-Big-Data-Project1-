import boto3
from minio import Minio

# Configuration MiniO
minio_client = Minio(
    "localhost:9000",
    access_key="admin",
    secret_key="password123",
    secure=False
)

# Créer un bucket
bucket_name = "datasets"
if not minio_client.bucket_exists(bucket_name):
    minio_client.make_bucket(bucket_name)
    print(f"Bucket {bucket_name} créé avec succès")

# Upload 
minio_client.fput_object(
    bucket_name,
    "raw_data/yellow_tripdata_2019-01.csv",
    "./data/yellow_tripdata_2019-01.csv"
)
print("Fichier uploadé dans MiniO")