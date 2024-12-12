import pandas as pd
from sqlalchemy import create_engine, text
from pymongo import MongoClient
import os
import xml.etree.ElementTree as ET  # For XML parsing
import pyarrow.parquet as pq  # For Parquet files
from src.backend.predefined_list import *
from src.backend.Table import Table
from src.backend.Database import Database
from src.backend.helper import *
from src.backend.nosql.generate_nosql_queries import *
from src.backend.sql.sql_query import *

user_inputs = []
 
def read_file(file_path):
    base_table_name = os.path.splitext(os.path.basename(file_path))[0]
    file_type = get_file_type(file_path)

    tables = []

    if 'excel' in file_type or 'spreadsheet' in file_type:
        xls = pd.ExcelFile(file_path)
        for sheet_name in xls.sheet_names:
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            sheet_table_name = f"{base_table_name}_{sheet_name}"

            new_table = Table(sheet_table_name, df)

            tables.append(new_table)

        new_database = Database(base_table_name, tables)

        return new_database

    if 'csv' in file_type:
        df = pd.read_csv(file_path)

    elif 'json' in file_type:
        df = pd.read_json(file_path)

        for column in df:
            df[column] = df[column].apply(convert_bson)

    elif 'tsv' in file_type:
        df = pd.read_csv(file_path, delimiter='\t')  # Read TSV file

    elif 'parquet' in file_type:
        table = pq.read_table(file_path)  # Read Parquet file using PyArrow
        df = table.to_pandas()  # Convert to pandas DataFrame

    elif 'xml' in file_type:
        tree = ET.parse(file_path)  # Parse XML file
        root = tree.getroot()
        data = [child.attrib for child in root]  # Convert XML elements to a list of dictionaries
        df = pd.DataFrame(data)

    else:
        print("Unsupported file format.")
        return

    new_table = Table(base_table_name, df)
    tables.append(new_table)
    return tables

def upload_dataset_to_rdbms(file_path, db_url, database):
    
    new_tables = read_file(file_path)
    # database = Database(database_name, tables)
    database_name = database.name
    database.create_tables(new_tables)
    engine = create_engine(db_url)
    with engine.connect() as connection:
        result = connection.execute(text(f"SHOW DATABASES LIKE '{database_name}';")).fetchall()
        if len(result) == 0:
            connection.execute(text(f"CREATE DATABASE {database_name};"))
            print(f"Database {database_name} created.")
        else:
            print(f"Database {database_name} already exists.")

    engine_with_db = create_engine(f"{db_url}/{database_name}")
    for table in new_tables:
        df = table.df
        df.to_sql(table.table_name, engine_with_db, if_exists='replace', index=False)

        print(f"Table '{table.table_name}' created or replaced in database '{database_name}'.\n")

    return new_tables

def upload_dataset_to_nosql(file_path, db_url, database):
    # Parse MongoDB connection URL
    client = MongoClient(db_url)
    new_tables = read_file(file_path)
    database.create_tables(new_tables)
    database_name = database.name
    client.drop_database(database_name)
    
    
    db = client[database_name]  # Create or select database
    for table in database.tables:
        df = table.df
        collection = db[table.table_name]  # Create or select collection
        collection.insert_many(df.to_dict("records"))  # Insert records

        print(f"Collection '{table.table_name}' created or replaced in database '{database_name}'.\n")

    return new_tables
