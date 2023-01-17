from google.cloud import storage
from google.oauth2 import service_account
from typing import List, Any
import io
import contextlib
import traceback
import sqlite3
import csv

class DatabaseSQLite:
    def __init__(self, db_name: str) -> None:
        self.conn = sqlite3.connect(db_name)

    @contextlib.contextmanager
    def get_connection(self):
        cur = self.conn.cursor()
        try:
            yield cur
        except Exception as e:
            traceback.print_exc()
        finally: 
            cur.close()    

class GoogleUpload:
    client = storage.Client
    
    def __init__(self, file: bytes, bucketname: str, credentials: str = "credentials.json") -> None:
        self.file = file
        self.CLOUD_STORAGE_BUCKET = bucketname
        self.set_credentials(credentials)
        
        
    def set_credentials(self, credentials: str):
        credentials = service_account.Credentials.from_service_account_file(credentials)
        self.client = storage.Client(credentials=credentials)

    def upload(self, path: str = "test/backup.csv"):
        bucket = self.client.get_bucket(self.CLOUD_STORAGE_BUCKET)
        
        blob = bucket.blob(path)
        
        blob.upload_from_string(
            self.file,
            content_type= "text/csv"
        )
        
        self.public_url = blob.public_url
        return blob.public_url


class HelperBackup:
    
    def __init__(self, db_name: str, bucketname: str, credentials: str = None) -> None:
        self.db_name = db_name
        self.bucketname = bucketname
        self.credentials = credentials
        self.db = DatabaseSQLite(self.db_name)
        self.upload = GoogleUpload
        
    @contextlib.contextmanager
    def run_query(self, query: str):
        with self.db.get_connection() as db:
            data = db.execute(query)
            header = [desc[0] for desc in data.description]
            return self.write_file_csv(data.fetchall(), header)        
            
    def write_file_csv(self, datas: List[Any], header: List[str]):
        with io.StringIO() as file:
            writer = csv.writer(file)
            writer.writerow(header)
            for data in datas:
                writer.writerow(data)
            yield self.upload(file=file.getvalue(), bucketname=self.bucketname, credentials = self.credentials)    
    
    
if __name__ == "__main__":
    
    query = "SELECT * FROM orders"
    helper = HelperBackup("data/database.db", "research_ai", "credentials.json")

    with helper.run_query(query) as query:
        path = "test/databackup.csv"
        res = query.upload(path)
        print(res)
