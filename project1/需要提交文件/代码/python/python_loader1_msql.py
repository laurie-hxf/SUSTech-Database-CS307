import pandas as pd
from sqlalchemy import create_engine
import mysql.connector

# === 修改为你的 MySQL 数据库信息 ===
db_user = 'root'
db_password = '123456'
db_host = 'localhost'
db_port = '3306'  # MySQL default port is 3306
db_name = 'project'  # Specify your MySQL database name

# === 创建连接引擎 ===
try:
    engine = create_engine(f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")
except Exception as e:
    print(e)
else:
    print("connect successful")

# === 读取原始 CSV 文件 ===
df = pd.read_csv("output25S.csv")
df.columns = df.columns.str.strip()  # 清理列名空格
print(df.columns)

# === 构建 company 表 ===
company_df = df[['client enterprise', 'country', 'city', 'industry']].drop_duplicates().reset_index(drop=True)
company_df = company_df.rename(columns={'client enterprise': 'client_enterprise'})
company_df.to_sql('company', engine, if_exists='append', index=False)
company_df['company_id'] = company_df.index + 1

# === 构建 supply_center 表 ===
supply_df = df[['supply center', 'director']].drop_duplicates().reset_index(drop=True)
supply_df = supply_df.rename(columns={'supply center': 'supply_center'})
supply_df.to_sql('supply_center', engine, if_exists='append', index=False)

# === 构建 contact 表 ===
contact_df = df[['contract number', 'contract date', 'supply center', 'client enterprise']].drop_duplicates().reset_index(drop=True)
contact_df = contact_df.rename(columns={
    'contract number': 'contract_number',
    'contract date': 'order_date',
    'client enterprise': 'client_enterprise',
    'supply center': 'supply_center',
})
contact_df.to_sql('contract', engine, if_exists='append', index=False)

# === 构建 salesman 表 ===
salesman_df = df[['salesman', 'salesman number', 'gender', 'age', 'mobile phone']].drop_duplicates().reset_index(drop=True)
salesman_df['salesman_id'] = salesman_df.index + 1
df = df.merge(salesman_df, on=['salesman', 'salesman number', 'gender', 'age', 'mobile phone'], how='left')
salesman_df = salesman_df.rename(columns={'salesman number': 'salesman_number', 'mobile phone': 'mobile_number'})
salesman_df.to_sql('salesman', engine, if_exists='append', index=False)

# === 构建 product 表 ===
product_df = df[['product code', 'product name']].drop_duplicates().reset_index(drop=True)
product_df = product_df.rename(columns={'product code': 'product_code', 'product name': 'product_name'})
product_df.to_sql('product', engine, if_exists='append', index=False)

# === 构建 product_model 表 ===
product_model_df = df[['product code', 'product model', 'unit price']].drop_duplicates().reset_index(drop=True)
product_model_df = product_model_df.rename(columns={
    'product code': 'product_code',
    'product model': 'product_model',
    'unit price': 'unit_price'
})
product_model_df.to_sql('product_model', engine, if_exists='append', index=False)

# === 构建 order 表 ===
order_df = df[['quantity', 'estimated delivery date', 'lodgement date', 'contract number', 'salesman_id', 'product code']].drop_duplicates().reset_index(drop=True)
order_df['order_id'] = order_df.index + 1
order_df = order_df.rename(columns={
    'estimated delivery date': 'estimated_delivery_date',
    'lodgement date': 'lodgement_date',
    'contract number': 'contract_number',
    'product code': 'product_code'
})
order_df.to_sql('orders', engine, if_exists='append', index=False)

print("✅ 所有数据已成功写入 MySQL 数据库！")
