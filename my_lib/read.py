from databricks import sql
import os
from dotenv import load_dotenv

def connect_db():
    # Get connection details from environment variables or replace with hardcoded values
    load_dotenv()
    server_hostname = os.getenv('DATABRICKS_SERVER_HOSTNAME')
    http_path = os.getenv('DATABRICKS_HTTP_PATH')
    access_token = os.getenv('DATABRICKS_ACCESS_TOKEN')

    try:
        # Establish a connection to Databricks SQL
        conn = sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token
        )
        return conn
    except Exception as e:
        print(f"Error connecting to Databricks: {e}")
        return None
