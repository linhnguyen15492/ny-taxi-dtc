import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_DATABASE = os.getenv("PG_DATABASE")


def get_connection(
    host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD, database=PG_DATABASE
):
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
        )

        return conn
    except psycopg2.Error as e:
        print(e)
        return None


def get_connection_string():
    return f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"


def main():
    conn = get_connection()
    if conn is not None:
        print("Connection to PostgreSQL database successful!")
        conn.close()
    else:
        print("Failed to connect to PostgreSQL database.")


if __name__ == "__main__":
    main()
