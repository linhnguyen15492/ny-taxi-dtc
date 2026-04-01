from datetime import datetime
import sys
import pandas as pd
import psycopg2
from tqdm import tqdm
import sqlalchemy
from utils import postgres_db
from google.cloud import storage


type = "yellow"
year = "2025"
month = "05"
URL = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{type}_tripdata_{year}-{month}.parquet"


def read_data_parquet(url: str) -> pd.DataFrame:
    return pd.read_parquet(url)


def data_generator(data: pd.DataFrame, chunk_size: int = 100000):
    length = len(data)
    for i in range(0, length, chunk_size):
        yield data[i : i + chunk_size]


def ingest_data_to_postgres(
    engine: sqlalchemy.Engine, table_name: str, data: pd.DataFrame
) -> bool:
    """
    Ingest data from the web.
    """
    try:
        data.to_sql(
            name=table_name,
            con=engine,
            if_exists="append",
            index=False,
        )
        print(f"Number of rows inserted into table {table_name}: {len(data)}")
        return True
    except Exception as e:
        print(f"Error occurred: {e}")
        return False


def ingest_tracking(
    conn: psycopg2.extensions.connection,
    source_name: str,
    num_rows: int,
    start_time: datetime,
    end_time: datetime,
):
    sql_script = """
                INSERT INTO ingest_tracking (source_name, num_rows, start_time, end_time)
                VALUES (%s, %s, %s, %s)
                """
    try:
        cursor = conn.cursor()
        cursor.execute(sql_script, (source_name, num_rows, start_time, end_time))

        conn.commit()
        conn.close()
        return True
    except Exception as e:
        conn.rollback()
        print(f"Error occurred while inserting file tracking data: {e}")
        return False


def upload_to_gcs(bucket_name: str, file_path: str, destination_blob_name: str) -> bool:
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(file_path)
        print(
            f"File {file_path} uploaded to {destination_blob_name} in bucket {bucket_name}."
        )
        return True
    except Exception as e:
        print(f"Error occurred while uploading file to GCS: {e}")
        return False


# upload_to_gcs("my-bucket", "local.txt", "remote/folder/new_name.txt")


def main():
    conn = postgres_db.get_connection()
    engine = sqlalchemy.create_engine(postgres_db.get_connection_string())

    # check connection
    if conn is None:
        sys.exit("Failed to connect to PostgreSQL database. Exiting.")

    # continue with data ingestion
    df = read_data_parquet(URL)
    nrows = 0
    try:
        start_time = datetime.now()
        for chunk in tqdm(data_generator(data=df)):
            ingest_data_to_postgres(
                engine=engine, table_name="yellow_tripdata", data=chunk
            )
            nrows += len(chunk)
        end_time = datetime.now()
        ingest_tracking(conn, URL, nrows, start_time, end_time)
        print(f"Total rows processed: {nrows} in {end_time - start_time}")

    except Exception as e:
        print(f"Error occurred during data ingestion: {e}")


if __name__ == "__main__":
    main()
