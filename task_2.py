from flask import Flask, request
import pandas as pd
import psycopg2
from datetime import datetime

app = Flask(__name__)


def create_table_name(prefix="master_study_list"):
    timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    return f"{prefix}_{timestamp}"


@app.route("/api/file-import", methods=["POST"])
def file_import():
    # Parse API request
    file = request.files["files"]
    create_usr_id = request.form["create_usr_id"]
    schema = request.form["schema"]

    # Generate table name
    table_name = create_table_name()

    # Read CSV file
    df = pd.read_csv(file)

    # Establish connection to PostgreSQL database
    connection = psycopg2.connect(
        dbname="your_dbname",
        user="your_username",
        password="your_password",
        host="your_host",
        port="your_port",
    )
    cursor = connection.cursor()

    # Create table in specified schema
    create_table_query = f"CREATE TABLE IF NOT EXISTS {schema}.{table_name} (LIKE public.master_study_list INCLUDING ALL)"
    cursor.execute(create_table_query)
    connection.commit()

    # Ingest data into table
    df.to_sql(table_name, connection, schema=schema, index=False, if_exists="append")

    # Close connection
    cursor.close()
    connection.close()

    return "CSV file imported successfully!"


if __name__ == "__main__":
    app.run(debug=True)
