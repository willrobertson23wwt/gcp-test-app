import os
import datetime
import sqlalchemy
from google.cloud.sql.connector import Connector
from google.cloud import storage

def main():
    # Environment variables set in Cloud Shell
    bucket_name = os.environ["BUCKET_NAME"]
    db_user = os.environ["DB_USER"]
    db_pass = os.environ["DB_PASS"]
    db_connection_name = os.environ["DB_INSTANCE_CONNECTION_NAME"]
    db_name = "myappdb"
    table_name = "entries"
    
    # Create a unique filename with a timestamp
    file_name = f"entry-{datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d_%H-%M-%S')}.txt"
    file_content = f"Timestamp: {datetime.datetime.now(datetime.timezone.utc)}"

    # 1. Upload to Cloud Storage
    print(f"Uploading {file_name} to bucket {bucket_name}...")
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.upload_from_string(file_content)
    print("Upload complete.")

    # 2. Insert into Cloud SQL
    print(f"Inserting record into {db_name}...")
    connector = Connector()
    
    def getconn():
        conn = connector.connect(
            db_connection_name,
            "pymysql",
            user=db_user,
            password=db_pass,
            db=db_name
        )
        return conn

    pool = sqlalchemy.create_engine("mysql+pymysql://", creator=getconn)
    
    with pool.connect() as db_conn:
        # Create table if it doesn't exist
        db_conn.execute(
            sqlalchemy.text(
                f"CREATE TABLE IF NOT EXISTS {table_name} ( "
                "id INT NOT NULL AUTO_INCREMENT, "
                "entry_time DATETIME NOT NULL, "
                "storage_path VARCHAR(255) NOT NULL, "
                "PRIMARY KEY (id) );"
            )
        )
        # Insert a new record
        insert_stmt = sqlalchemy.text(
            f"INSERT INTO {table_name} (entry_time, storage_path) VALUES (:entry_time, :storage_path)"
        )
        db_conn.execute(insert_stmt, parameters={"entry_time": datetime.datetime.now(datetime.timezone.utc), "storage_path": f"gs://{bucket_name}/{file_name}"})
        db_conn.commit()

    print("Record inserted successfully.")
    connector.close()

if __name__ == "__main__":
    main()
