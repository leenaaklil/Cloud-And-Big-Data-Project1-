from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import boto3
import psycopg2
import io

def etl_pipeline():
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="admin",
        aws_secret_access_key="password123"
    )

    bucket = "datasets"
    key = "yellow_tripdata_2019-01.csv"

    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(io.BytesIO(obj["Body"].read()))

    df = df.dropna().drop_duplicates()
    df = df[(df["trip_distance"] > 0) & (df["trip_distance"] < 100)]

    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    df["trip_duration"] = (
        df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]
    ).dt.total_seconds() / 3600

    df = df[df["trip_duration"] > 0]
    df["avg_speed"] = df["trip_distance"] / df["trip_duration"]

    final_df = df.groupby("VendorID").agg({
        "passenger_count": "mean",
        "trip_distance": "mean",
        "total_amount": "mean",
        "avg_speed": "mean"
    }).reset_index()

    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow"
    )

    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE nyc_taxi_analytics")

    for _, row in final_df.iterrows():
        cur.execute(
            "INSERT INTO nyc_taxi_analytics VALUES (%s,%s,%s,%s,%s)",
            tuple(row)
        )

    conn.commit()
    cur.close()
    conn.close()

with DAG(
    "nyc_taxi_minio_postgres_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
):
    PythonOperator(
        task_id="run_etl",
        python_callable=etl_pipeline
    )
