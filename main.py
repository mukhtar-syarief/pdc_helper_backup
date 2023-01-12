from google.cloud import storage
import io
import contextlib
import traceback
import sqlite3

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
    
    def __init__(self, file: bytes, bucketname: str = "tokped_research", credentials = None) -> None:
        self.file = file
        self.client = storage.Client(credentials=credentials)
        self.CLOUD_STORAGE_BUCKET = bucketname

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
    
    def __init__(self, db_name: str, bucketname: str, credential = None) -> None:
        self.db_name = db_name
        self.bucketname = bucketname
        self.credentiala = credential 
        self.db = DatabaseSQLite(self.db_name)
        self.upload = GoogleUpload
        
    @contextlib.contextmanager
    def run_query(self, query: str):
        with self.db.get_connection() as db:
            data = db.execute(query)
            return self.write_file_csv(data.fetchall())        
            
    def write_file_csv(self, datas):
        list_data = []
        for data in datas:
            data_list= list(data)
            data_str = ",".join(map(str, data_list))
            list_data.append(data_str)
        data_str = "\n".join(list_data)
        file_bytes = io.BytesIO(data_str.encode("utf-8"))
        yield self.upload(file=file_bytes.read(), bucketname=self.bucketname, credentials = self.credentiala)
    
    
if __name__ == "__main__":
    
    query = "SELECT * FROM akun JOIN orders WHERE akun.id = orders.shop_id"
    helper = HelperBackup("data/database.db", "tokped_research")
    
    with helper.run_query(query) as query:
        path = "csv/test/databackup.csv"
        res = query.upload(path)
        print(res)
        
    # from pprint import pprint
    # with open("orders.csv", "r") as order:
    #     datas = order.read().split("\n")
    #     for data in datas:
    #         data = data.split(",")
    #         pprint(data)
    
    
# class Database:
#     def __init__(self, db_name: str) -> None:
#         self.DATABASE_URI = os.environ.get('DATABASE_URI', f'sqlite://{db_name}')
        
#     def set_session(self):
#         engine = create_engine(
#             self.DATABASE_URI,
#             connect_args={"check_same_thread": False}
#         )

#         Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
#         return engine
    
#     @contextlib.contextmanager
#     def get_db(self):
#         engine = self.set_session()
#         db = engine.connect()
#         try:
#             # print("get_db")
#             yield db
#         except Exception as e:
#             db.rollback()
#             traceback.print_exc()
#         finally:
#             db.close()