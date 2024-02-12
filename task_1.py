import pandas as pd
import psycopg2
from psycopg2 import sql


def ingest_large_csv(csv_file, table_name, connection):
    chunksize = 10000  # Adjust chunk size based on system memory and performance

    # Establish connection to PostgreSQL database
    with connection.cursor() as cursor:
        # Create table if not exists
        cursor.execute(
            sql.SQL(
                "CREATE TABLE IF NOT EXISTS {} (LIKE public.test_od INCLUDING ALL)"
            ).format(sql.Identifier(table_name))
        )

    # Read CSV in chunks and insert into PostgreSQL table
    for chunk in pd.read_csv(csv_file, chunksize=chunksize):
        chunk.to_sql(table_name, connection, if_exists="append", index=False)


if __name__ == "__main__":
    # Connection parameters
    dbname = "your_dbname"
    user = "your_username"
    password = "your_password"
    host = "your_host"
    port = "your_port"

    # Connect to PostgreSQL database
    connection = psycopg2.connect(
        dbname=dbname, user=user, password=password, host=host, port=port
    )

    # Ingest large CSV into PostgreSQL table
    ingest_large_csv("large_file.csv", "public.test_od", connection)

    # Close connection
    connection.close()
