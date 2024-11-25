import sqlite3
import logging
import pickle


logging.basicConfig(filename='./log.txt',level="WARNING")

# 数据库文件路径
DB_PATH = 'kv_cache.db'

class KV:
    '''
    数据库抽象层
    提供数据库初始化和数据查询
    '''
    def __init__(self, db_path:str=DB_PATH):
        self.db_path = db_path
        self.create_tables()

    def create_tables(self):
        with self.db_connection() as cursor:
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS cache (
                key TEXT PRIMARY KEY,
                value BLOB
            )
            ''')

            cursor.connection.commit()


    def db_connection(self):
        class DBConnection:
            def __init__(self, db_path):
                self.db_path = db_path

            def __enter__(self):
                self.conn = sqlite3.connect(self.db_path)
                self.cursor = self.conn.cursor()
                return self.cursor

            def __exit__(self, exc_type, exc_val, exc_tb):
                self.conn.close()

        return DBConnection(self.db_path)


    def get(self, key:str):
        with self.db_connection() as cursor:
            cursor.execute('''
                SELECT value FROM cache 
                WHERE key = ?;''',(str(key),)
            )
            cached_result = cursor.fetchone()
            if not cached_result:
                return None
            try:
                # 尝试反序列化数据
                return pickle.loads(cached_result[0])
            except:
                # 如果反序列化失败，说明数据是 bytes 类型
                return cached_result[0]

    def set(self, key, value):
        serialized_data = None
        if isinstance(value, bytes):
            # 如果数据是 bytes 类型，直接存储
            serialized_data = value
        else:
            # 否则，使用 pickle 序列化数据
            serialized_data = pickle.dumps(value)
        with self.db_connection() as cursor:
            cursor.execute('''
            INSERT OR REPLACE INTO cache (key, value)
            VALUES (?, ?)
            ''', (str(key), serialized_data,))
            cursor.connection.commit()



# 测试
if __name__ == "__main__":
    kv = KV()
    for i in range(100):
        r = kv.get(i)
        if r:
            print("cached",r)
        else:
            r = i*i
            kv.set(i,r)
            print("new",r)