import pandas as pd
import pymysql
from import_export_pipeline.import_export import KayBeeExports
from dotenv import load_dotenv
import os

load_dotenv(".env")
# 'BABY CORN','POMEGRANATES ARILS', 'POMEGRANATES',"CHICKOO","GUAVA","CHILLI","MANGO","DRAGON FRUITS","OKRA","DRUMSTICK","MIX FRUITS & VEG","COCONUT",

def fetch_data_from_mysql(query, connection_params):
    conn = pymysql.connect(**connection_params)
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def all_report_from_mysql(query, connection_params,report_name):
    data = fetch_data_from_mysql(query, connection_params)
    df = KayBeeExports(data)
    
    a = df.apply_regex()
    a.to_excel(rf"D:\UserProfile\Desktop\cleaned_data_{report_name}.xlsx", index=False)

    exporter_report_path = r"D:\UserProfile\Desktop\EXPORTER_REPORT.xlsx"
    df.generate_exporter_report(exporter_report_path)

    product_report_path = rf"D:\UserProfile\Desktop\PRODUCT_REPORT_{report_name}.xlsx"
    df.generate_product_report(product_report_path)

    importer_report_path = r"D:\UserProfile\Desktop\IMPORTER_REPORT.xlsx"
    df.generate_importer_report(importer_report_path)

connection_params = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_DATABASE')
}

query = """
SELECT *
FROM import_export
WHERE foreign_country LIKE 'Norway'
  AND date BETWEEN '2022-01-01' AND '2024-07-31';
"""

all_report_from_mysql(query, connection_params,report_name='Norway')
