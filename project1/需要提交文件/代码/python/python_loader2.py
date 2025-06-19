import json
import psycopg2
from psycopg2 import sql
from datetime import datetime, timedelta
import time
import concurrent.futures
import time


db = ['localhost', '5432', '', '', 'cs307_project1']

sql_files = [
    '../sql/company.sql',
    '../sql/contract.sql',
    '../sql/orders.sql',
    '../sql/product_model.sql',
    '../sql/product.sql',
    '../sql/salesman.sql',
    '../sql/supply_center.sql',
    '../sql/disable_trigger.sql',
]

dis_triggers = open(sql_files[7]).read()
company = open(sql_files[0]).read()
contract = open(sql_files[1]).read()
orders = open(sql_files[2]).read()
product_model = open(sql_files[3]).read()
product = open(sql_files[4]).read()
salesman = open(sql_files[5]).read()
supply_center = open(sql_files[6]).read()

def execute_sql_file(filename):
    sql = open(filename).read()
    conn = psycopg2.connect(host=db[0], port=db[1], user=db[2], password=db[3], database=db[4])
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()

def multi_thread_import():

    with concurrent.futures.ThreadPoolExecutor() as executor:
        print("Importing all data...")
        executor.map(execute_sql_file, sql_files)

def import_all():
    start_time = time.time()

    conn = psycopg2.connect(host=db[0], port=db[1], user=db[2], password=db[3], database=db[4])
    cur = conn.cursor()

    cur.execute(dis_triggers)
    cur.execute(company)
    cur.execute(supply_center)
    cur.execute(contract)
    cur.execute(salesman)
    cur.execute(product)
    cur.execute(orders)
    cur.execute(product_model)

    conn.commit()
    cur.close()
    conn.close()
    end_time = time.time()
    print(end_time - start_time)



if __name__ == '__main__':
    start = time.perf_counter()
    # Load All Data
    # import_all()

    # Multi-thread Import All Data with disable triggers
    multi_thread_import()
    end = time.perf_counter()
    print(f"耗时：{end - start:.4f} 秒")
    pass