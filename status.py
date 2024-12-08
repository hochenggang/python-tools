import os
import time
import pickle
import logging
import sqlite3
from functools import wraps

# init log
logging.basicConfig(
    filename='./log.txt',
    level=logging.WARNING,
    format='%(asctime)s - %(levelname)s - %(funcName)s - %(message)s'
)

def decorator_logging_timer():
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            t1 = time.time()
            try:
                result = func(*args, **kwargs)
            except Exception as e:
                raise e
            finally:
                logging.warning(f"{time.strftime('%Y-%m-%d %H:%M:%S')} function <{func.__name__}> finished in {time.time()-t1} ms.")
            return result
        return wrapper
    return decorator


class sqlite_cursor:
    def __init__(self, db_path):
        self.db_path = db_path
        if not self.db_path:
            raise ValueError("@db_path required.")

    def __enter__(self):
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.cursor = self.conn.cursor()
            return self.cursor
        except sqlite3.Error as e:
            logging.error(f"unable to connect {self.db_path}: {e}")
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            logging.error(f"when context manager quiting, error happend: {exc_val}")

        try:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
            logging.debug("database closed.")
        except sqlite3.Error as e:
            logging.error(f"error on closing database: {e}")
            



class KV:
    '''
    a simple key and value storage class based on sqlite and pickle
    '''
    def __init__(self, db_path:str=os.path.join(os.getcwd(),'status.db')):
        self.db_path = db_path
        self.create_tables()

    def create_tables(self):
        with sqlite_cursor(self.db_path) as cursor:
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS cache (
                key TEXT PRIMARY KEY,
                value BLOB
            )
            ''')
            cursor.connection.commit()

    def get(self, key:str):
        with sqlite_cursor(self.db_path) as cursor:
            cursor.execute('''
                SELECT value FROM cache 
                WHERE key = ?;''',(str(key),)
            )
            cached_result = cursor.fetchone()
            if not cached_result:
                return None
            return pickle.loads(cached_result[0])
            
    def set(self, key:str, value):
        with sqlite_cursor(self.db_path) as cursor:
            cursor.execute('''
            INSERT OR REPLACE INTO cache (key, value)
            VALUES (?, ?)
            ''', (str(key), pickle.dumps(value)))
            cursor.connection.commit()
    
    def delete(self, key:str):
        with sqlite_cursor(self.db_path) as cursor:
            cursor.execute('''
            DELETE FROM cache WHERE key=?;
            ''', (str(key), )
            )
            cursor.connection.commit()

default_kv = KV()

# test
if __name__ == "__main__":
    kv = default_kv
    for i in range(100):
        r = kv.get(i)
        if r:
            print("cached",r)
        else:
            r = i*i
            kv.set(i,r)
            print("new",r)
