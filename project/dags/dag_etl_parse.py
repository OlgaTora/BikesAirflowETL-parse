import pandas as pd
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from config.config import db, input_path, archive_path
from dags.etl.cleaner import Cleaner
from dags.etl.transform import Transform

from etl.connection import Connection
from etl.sqlreader import SQLReader
from etl.extractdata import ExtractData
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from sql_scripts.union_tables import union_equal_tables
from dags.parser.parser import Parser


@dag(
    schedule=None,
    start_date=days_ago(0),
    catchup=False,
    tags=['etl', 'parser'],
)
def etl_and_parse():
    @task()
    def extract_data_from_files() -> list:
        connect = Connection(db).create_connection()
        extract = ExtractData(input_path, archive_path, connect)
        tables_list = extract.create_stg_tables()
        return tables_list

    @task()
    def create_dwh(tables_list) -> list:
        dwh_tables_list = []
        connect = Connection(db).create_connection()
        for table in tables_list:
            data = pd.read_sql_query(f"""Select * from {table}""", con=connect)
            transformer = Transform(data)
            transform_data = transformer.transform()
            transform_data.to_sql(f'DWH_{table[4:]}', con=connect, if_exists='append', index=False)
            dwh_tables_list.append(f'DWH_{table[4:]}')
        return dwh_tables_list

    @task()
    def create_data_mart(tables_list: list) -> str:
        connect = Connection(db).create_connection()
        frames = []
        for table in tables_list:
            data = pd.read_sql_query(f"""Select * from {table}""", con=connect)
            frames.append(data)
        if len(frames) > 1:
            result = pd.merge(frames[0], pd.merge(frames[1], frames[2], on='customer_id'), on='customer_id')
        else:
            result = frames[0]
        cleaner = Cleaner(result)
        result = cleaner.cleaning()
        result.to_sql('DM_Sales', con=connect, if_exists='replace')
        return 'DM_Sales'

    @task()
    def extract_web_data(category_name):
        connect = Connection(db).create_connection()
        parser = Parser(category_name)
        data = parser.take_all_files()
        data['category'] = category_name
        table_name = category_name.replace('-', '_')
        table_name = f'STG_{table_name}'
        data.to_sql(table_name, con=connect, if_exists='replace')
        return table_name

    @task()
    def save_parse_data2dwh(list_tables):
        connect = Connection(db)
        reader = SQLReader(connect)
        tables = union_equal_tables(list_tables)
        s = f"""
        Create table if not EXISTS DWH_parser as {tables}
        """
        reader.execute_sql_script(s, file=False)
        return 'DWH_parser'

    data = extract_data_from_files()
    dwh_tables_list = create_dwh(data)
    mart_table = create_data_mart(dwh_tables_list)

    category_list = ['velosipednie-sumki', 'fonari', 'zamki', 'krilya', 'flagi', 'bagajniki']
    extract_data = extract_web_data.expand(category_name=category_list)
    dwh_tables = save_parse_data2dwh(extract_data)


prod = etl_and_parse()
