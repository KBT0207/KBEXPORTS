import pandas as pd
import pymysql
from import_export import KayBeeExports

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
    'host': 'localhost',
    'user': 'root',
    'password': '1234',
    'database': 'kbexports'
}

query = """
SELECT *
FROM import_export
WHERE foreign_country LIKE 'Germany'
  AND date BETWEEN '2023-06-01' AND '2024-07-01';


"""

all_report_from_mysql(query, connection_params,report_name='Germany')
