# In Cloud Composer, add apache-airflow-providers-snowflake and yfinance to PYPI Packages
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta
from datetime import datetime
import snowflake.connector
import yfinance as yf
import pandas as pd

# NEW
# DBT_PROJECT_DIR = "/opt/airflow/lab02_pair14"

def return_snowflake_conn():
    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_connect')
    
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()

@task
def extract_stock_data(stock_symbols):

    """Extract raw stock data from yfinance"""
    if isinstance(stock_symbols, str):
        stock_symbols = [stock_symbols]

    all_stock_data = {}

    for stock_symbol in stock_symbols:
        stock = yf.Ticker(stock_symbol)
        hist = stock.history(period="180d")
        all_stock_data[stock_symbol] = hist

    return all_stock_data

@task
def transform_stock_data(stock_data_dict):
    """Transform the raw data into the required format"""
    all_results = []

    for stock_symbol, hist in stock_data_dict.items():
        try:
            # Convert the dataframe to the expected format
            for index, row in hist.iterrows():
                #date_str = index.strftime('%Y-%m-%d')
                stock_info = {
                    "Stock Symbol": stock_symbol,
                    "Date": index.date().isoformat(),
                    "Open": row["Open"],
                    "High": row["High"],
                    "Low": row["Low"],
                    "Close": row["Close"],
                    "Volume": row["Volume"]
                }
                all_results.append(stock_info)
        except Exception as e:
            print(f"Error transforming data for {stock_symbol}: {str(e)}")
            continue

    return all_results

@task 
def load_records_into_snowflake(results):
    con = return_snowflake_conn()

    target_table = "USER_DB_BOA.raw.stocks_data"
    try:
        con.execute("BEGIN;")
        # con.execute("CREATE DATABASE IF NOT EXISTS USER_DB_BOA;")

        # Use the database before creating schemas
        con.execute("USE DATABASE USER_DB_BOA;")
        
        con.execute("CREATE SCHEMA IF NOT EXISTS raw;")
        con.execute("CREATE SCHEMA IF NOT EXISTS analytics;")

        con.execute(f"""CREATE TABLE IF NOT EXISTS {target_table} (
          symbol STRING,
          date DATE,
          open FLOAT,
          high FLOAT,
          low FLOAT,
          close FLOAT,
          volume INT,
          PRIMARY KEY (symbol, date));""")
        
        con.execute(f"""DELETE FROM {target_table}""")

        tuples_data = [
            (
                row["Stock Symbol"],
                row["Date"],
                row["Open"],
                row["High"],
                row["Low"],
                row["Close"],
                row["Volume"]
            )
            for row in results
        ]

        print("Sample insert data:", tuples_data[:3])

        print("Total records to insert:", len(results))
        print("Sample record:", results[0] if results else "No records")

        con.executemany(
            f"""INSERT INTO {target_table} (SYMBOL, DATE, OPEN, HIGH, LOW, CLOSE, VOLUME) 
               VALUES (%s, %s, %s, %s, %s, %s, %s)""", tuples_data)

        con.execute("COMMIT;")
    except Exception as e:
        con.execute("ROLLBACK;")
        print(e)
        raise e

with DAG(
    dag_id = 'Lab01_FetchStockData_YFinance',
    start_date = datetime(2025,3,1),
    catchup=False,
    tags=['ETL'],
    schedule = '30 2 * * *'
) as dag:

    # Defining stock symbol and calling the function
    stock_symbols = ["NVDA","AAPL"]

    # Execute the ETL pipeline with separate tasks
    extracted_data = extract_stock_data(stock_symbols)
    transformed_data = transform_stock_data(extracted_data)
    load_records_into_snowflake(transformed_data)

    
