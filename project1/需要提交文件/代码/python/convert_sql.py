import csv
import time
import os
import sys
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy import table




def csv_to_sql(csv_path,list,table_name,headers):
    sql_file_path = "../"+table_name+".sql"
    with open(csv_path, mode='r', encoding='utf-8-sig') as csv_file:
        reader = csv.reader(csv_file)
        head = next(reader)
        seen = set()
        salesman_temp = dict()
        # salesman2 = set()
        sql_lines = []
        i = 1
        j = 1
        k = 1
        for row in reader:
            temp = []
            if table_name == "salesman":
                temp.append(str(i))
            elif table_name == "orders":
                temp.append(str(j))
            for i in list:
                row[i] = row[i].strip()
                if row[i] == '':
                    temp.append("null")
                elif row[i].replace('.', '', 1).isdigit():
                    temp.append(row[i])
                else:
                    # 转义单引号
                    row[i] = row[i].replace("'", "''")
                    temp.append(f"'{row[i]}'")
            if tuple(temp)  in seen:
                continue
            seen.add(tuple(temp))
            if table_name == "orders":
                if row[13] =="2025-3-25":
                    row[13] = "null"
                    temp.append(row[13])
                if row[15] not in salesman_temp:
                    salesman_temp[row[15]] = k
                    k+=1
                temp.append(str(salesman_temp[row[15]]))
            line = f"INSERT INTO {table_name} ({', '.join(headers)}) VALUES ({', '.join(temp)});"
            sql_lines.append(line)
            i+=1
            j+=1


        with open(sql_file_path, mode='w', encoding='utf-8') as sql_file:
            sql_file.write('\n'.join(sql_lines))
        print(f"已生成 SQL 文件：{sql_file_path}")





# 示例用法
if __name__ == "__main__":
    csv_path = "../output25S.csv"
    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=7) as executor:

        futures = []
        futures.append(executor.submit(csv_to_sql,csv_path,[1,3,4,5],"company",["client_enterprise", "country", "city", "industry"]))
        futures.append(executor.submit(csv_to_sql,csv_path,[0,11,2,1],"contract",["contract_number", "order_date", "supply_center", "client_enterprise"]))
        futures.append(executor.submit(csv_to_sql,csv_path,[10,12,0,6],"orders",["order_id, quantity"," estimated_delivery_date"," contract_number","product_code"," lodgement_date","salesman_id"]))
        futures.append(executor.submit(csv_to_sql,csv_path,[6,7],"product",["product_code", "product_name"]))
        futures.append(executor.submit(csv_to_sql,csv_path,[6,8,9],"product_model",["product_code", "product_model", "unit_price"]))
        futures.append(executor.submit(csv_to_sql,csv_path,[16,15,17,18,19],"salesman",["salesman_id", "salesman_number", "salesman", "gender", "age", "mobile_number"]))
        futures.append(executor.submit(csv_to_sql,csv_path,[2,14],"supply_center",["supply_center", "director"]))


        # 等待所有任务完成
        for future in futures:
            future.result()

    # csv_to_sql(csv_path,[1,3,4,5],"company",["client_enterprise", "country", "city", "industry"])
    # csv_to_sql(csv_path,[0,11,2,1],"contract",["contract_number", "order_date", "supply_center", "client_enterprise"])
    # csv_to_sql(csv_path,[10,12,0,6],"orders",["order_id, quantity"," estimated_delivery_date"," contract_number","product_code"," lodgement_date","salesman_id"])
    # csv_to_sql(csv_path,[6,7],"product",["product_code", "product_name"])
    # csv_to_sql(csv_path,[6,8,9],"product_model",["product_code", "product_model", "unit_price"])
    # csv_to_sql(csv_path,[16,15,17,18,19],"salesman",["salesman_id", "salesman_number", "salesman", "gender", "age", "mobile_number"])
    # csv_to_sql(csv_path,[2,14],"supply_center",["supply_center", "director"])

    end = time.perf_counter()
    print(f"耗时：{end - start:.4f} 秒")