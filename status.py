import os
import time
import pickle
import logging
import sqlite3
from functools import wraps

# 初始化日志配置
logging.basicConfig(
    filename='./log.txt',
    level=logging.WARN,
    format='%(asctime)s - %(levelname)s - %(funcName)s - %(message)s'
)

def decorator_logging_timer():
    '''
    装饰器
    使用日志记录函数的执行时间
    '''
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



# 封装数据库 cursor
class sqlite_cursor:
    def __init__(self, db_path):
        self.db_path = db_path
        if not self.db_path:
            raise ValueError("数据库路径不能为空")

    def __enter__(self):
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.cursor = self.conn.cursor()
            logging.debug(f"成功连接到数据库: {self.db_path}")
            return self.cursor
        except sqlite3.Error as e:
            logging.error(f"连接数据库时发生错误: {e}")
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            logging.error(f"执行SQL时发生异常: {exc_val}")

        try:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
            logging.debug("数据库连接已关闭")
        except sqlite3.Error as e:
            logging.error(f"关闭数据库连接时发生错误: {e}")
            
            
# 准备状态管理
class KV:
    '''
    数据库抽象层
    提供数据库初始化和数据查询
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

# 测试
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