import duckdb
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

PATH = os.path.join(PROJECT_ROOT, "db/taxi.duckdb")

def get_duckdb_connection(db_path):
    file_path = Path(db_path)
    if file_path.is_file():
        print(f"The file {file_path} exists and is a file.")
        try:
            conn = duckdb.connect(database=db_path)
            print(f"Successfully connected to DuckDB at {db_path}")
            return conn
        except Exception as e:
            print(f"Error connecting to DuckDB: {e}")
            return None
    elif file_path.exists():
        print(f"The path {file_path} exists but is not a file (e.g., a directory).")
    else:
        print(f"The file {file_path} does not exist.")

    return None



def main():
    conn = get_duckdb_connection(PATH)
    if conn is not None:
        print("Connection to DuckDB successful!")
        # You can perform database operations here
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
    else:
        print("Failed to connect to DuckDB.")

if __name__ == "__main__":
    main()
