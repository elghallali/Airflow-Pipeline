from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import logging

# Function to check if it's the first execution
def is_first_dag_run(**kwargs):
    # Check if the task has already run
    task_has_run = Variable.get("historical_data_extracted", default_var=False)
    
    # If the task hasn't run before, we will mark it as True after the first execution
    if not task_has_run:
        Variable.set("historical_data_extracted", True)
        return True
    return False

# Function to extract historical data (run only the first time)
def extract_historical_data_task(tickers, **kwargs):
    logging.info("Running full historical data extraction.")
    tickers = yf.Tickers(tickers)
    data = {}

    for ticker, ticker_data in tickers.tickers.items():
        try:
            df = ticker_data.history(period="max", interval="1d")  # Fetch full historical data
            if not df.empty:
                df['Ticker'] = ticker
                data[ticker] = df
        except Exception as e:
            logging.error(f"Error fetching historical data for {ticker}: {str(e)}")
            continue

    return data

# Function to extract real-time data (run in subsequent executions)
def extract_real_time_data_task(tickers, **kwargs):
    logging.info("Running daily data update.")
    tickers = yf.Tickers(tickers)
    data = {}

    # Fetch the latest day's data
    for ticker, ticker_data in tickers.tickers.items():
        try:
            start = (datetime.today() - timedelta(days=1)).date()
            end = datetime.today().date()
            df = ticker_data.history(start=start, end=end, interval="1d")
            if not df.empty:
                df['Ticker'] = ticker
                data[ticker] = df
        except Exception as e:
            logging.error(f"Error fetching daily data for {ticker}: {str(e)}")
            continue

    return data

# Main function to decide which task to run
def conditional_data_extraction(tickers, **kwargs):
    if is_first_dag_run(**kwargs):
        # Run historical data extraction on the first run
        data = extract_historical_data_task(tickers, **kwargs)
    else:
        # Run real-time data extraction in subsequent runs
        data = extract_real_time_data_task(tickers, **kwargs)
    kwargs['ti'].xcom_push(key='extract_daily_data', value=data)

# Function to load data into PostgreSQL (Upsert)
def load_data_to_postgres_task(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='transformed_data', key='transformed_data')
    postgres_hook = PostgresHook(postgres_conn_id='stock_connection')

    for ticker, df in data.items():
        for index, row in df.iterrows():
            
            insert_query = f"""
            INSERT INTO daily_stock_data (ticker, date, open, high, low, close, volume, source)
            VALUES ('{row['Ticker']}', '{row['Date']}', {row['Open']}, {row['High']}, {row['Low']}, {row['Close']}, {row['Volume']}, 'Yahoo Finance')
            ON CONFLICT (ticker, date)
            DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume;
            """
            postgres_hook.run(insert_query)


# Function to transform data (optional)
def transform_data_task(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='extract_daily_stock_data', key='extract_daily_data')
    transformed_data = {}

    for ticker, df in data.items():
        df.reset_index(inplace=True)
        transformed_data[ticker] = df

    kwargs['ti'].xcom_push(key='transformed_data', value=transformed_data)

# Define the DAG
with DAG(dag_id='yahoo_finance_etl_daily_stocks_data_1',
         start_date=datetime(2024, 12, 10),
         schedule_interval='@daily',
         catchup=False) as dag:

    create_table_task = PostgresOperator(
        task_id='create_daily_stock_table',
        postgres_conn_id='stock_connection',
        sql="""
        CREATE TABLE IF NOT EXISTS daily_stock_data (
            id SERIAL PRIMARY KEY,
            ticker VARCHAR(10),
            date DATE,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume BIGINT,
            source VARCHAR(255),
            CONSTRAINT unique_ticker_daily_date UNIQUE (ticker, date)  -- Ensure no duplicates
        );
        """
    )

    extract_data = PythonOperator(
        task_id='extract_daily_stock_data',
        python_callable=conditional_data_extraction,
        op_kwargs={'tickers': ['MSFT', 'AAPL', 'GOOG', 'NVDA', 'AMZN', 'META', 'NFLX', 'TSLA', 'ACN']}
    )

    transform_data = PythonOperator(
        task_id='transformed_data',
        python_callable=transform_data_task
    )

    load_data_to_postgres = PythonOperator(
        task_id='load_data_to_postgres',
        python_callable=load_data_to_postgres_task
    )

    create_table_task >> extract_data >> transform_data >> load_data_to_postgres