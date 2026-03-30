import duckdb
from pathlib import Path

PATH = "../db/taxi.duckdb"

file_path = Path(PATH)

if file_path.is_file():
    print(f"The file {file_path} exists and is a file.")
elif file_path.exists():
    print(f"The path {file_path} exists but is not a file (e.g., a directory).")
else:
    print(f"The file {file_path} does not exist.")

conn = duckdb.connect(database=PATH)

conn.sql(
    "create table if not exists file_data (file_name varchar primary key, file_path varchar, ingestion_time timestamp)"
)

conn.sql("create index if not exists idx_file_name on file_data(file_name)")

conn.sql(
    "insert into file_data values ('yellow_tripdata_2021-01.csv', '../data/yellow_tripdata_2021-01.csv', current_timestamp)"
)

conn.sql("select * from file_data")

conn.table("file_data").show()

# explicitly close the connection
conn.close()


def ingest_data_web():
    """
    Ingest data from the web.
    """
    pass
