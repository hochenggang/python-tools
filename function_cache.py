import sqlite3
import time
import hashlib
import json
from functools import wraps
import logging

logging.basicConfig(filename='./log.txt',level="INFO")

# 数据库文件路径
DB_PATH = 'function_cache.db'

class DBManager:
    '''
    数据库抽象层
    提供数据库初始化和数据查询
    '''
    def __init__(self, db_path):
        self.db_path = db_path
        self.create_tables()

    def create_tables(self):
        with self.db_connection() as cursor:
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS cache (
                function_name TEXT,
                args_hash TEXT,
                args TEXT,
                result TEXT,
                call_date TEXT,
                call_timestamp INTEGER,
                PRIMARY KEY (function_name, args_hash)
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

    def get_cached_result(self, function_name, args_hash):
        with self.db_connection() as cursor:
            cursor.execute('''
            SELECT result FROM cache
            WHERE function_name = ? AND args_hash = ?
            ''', (function_name, args_hash))
            cached_result = cursor.fetchone()
            return cached_result

    def get_call_count(self, function_name, call_date):
        with self.db_connection() as cursor:
            cursor.execute('''
            SELECT COUNT(*) FROM cache
            WHERE function_name = ? AND call_date = ?
            ''', (function_name, call_date))
            call_count = cursor.fetchone()[0]
            return call_count

    def insert_cache(self, function_name, args_hash, args, result, call_date, call_timestamp):
        with self.db_connection() as cursor:
            cursor.execute('''
            INSERT OR REPLACE INTO cache (function_name, args_hash, args, result, call_date, call_timestamp)
            VALUES (?, ?, ?, ?, ?, ?)
            ''', (function_name, args_hash, json.dumps(args), json.dumps(result), call_date, call_timestamp))
            cursor.connection.commit()


def hash_args(*args):
    return hashlib.md5(json.dumps(args, sort_keys=True).encode()).hexdigest()


def limit_calls_per_day_with_cache(max_calls=99):
    '''
    装饰器
    缓存函数结果
    并进行限流
    用在花销较大的函数上可以节省成本
    '''
    def decorator(func):
        db_manager = DBManager(DB_PATH)

        @wraps(func)
        def wrapper(*args, **kwargs):
            today = time.strftime('%Y-%m-%d')
            now_timestamp = int(time.time() * 1000)  # 获取当前毫秒级时间戳
            args_hash = hash_args(*args)
            function_name = func.__name__

            # 检查缓存
            cached_result = db_manager.get_cached_result(function_name, args_hash)
            if cached_result:
                cached_result = cached_result[0]
                return json.loads(cached_result)

            # 检查当天调用次数
            call_count = db_manager.get_call_count(function_name, today)
            if call_count >= max_calls:
                raise Exception(f"Function {function_name} has reached the daily limit of {max_calls} calls.")

            try:
                # 调用函数并缓存结果
                result = func(*args, **kwargs)
                result_to_cache = result
            except Exception as e:
                result_to_cache = str(e)
                raise e
            finally:
                # 更新缓存
                db_manager.insert_cache(function_name, args_hash, args, result_to_cache, today, now_timestamp)
            
            return result

        return wrapper
    return decorator



def timer():
    '''
    装饰器
    记录函数的执行时间
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
                logging.info(f"{time.strftime('%Y-%m-%d %H:%M:%S')} function <{func.__name__}> finished in {time.time()-t1}")
            return result

        return wrapper
    return decorator

    
# 示例函数
@limit_calls_per_day(max_calls=999)
def test_task11(*args):
    if sum(args) > 550:
        raise ValueError("Sum of arguments is too large")
    return [sum(args),args]


@timer()
def test():
    try:
        r = []
        for i in range(500):
            r.append(test_task11(i, i-1))
        return r
    except Exception as e:
        return e

# 测试
if __name__ == "__main__":
    print(test())