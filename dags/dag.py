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
    logging.info("Running historical data extraction.")
    tickers = yf.Tickers(tickers)
    data = {}

    for k in tickers.tickers.keys():
        df_list = []

        for i in range(30):
            start = datetime.today() - timedelta(days=30-i)
            end = start + timedelta(days=1)

            try:
                df = tickers.tickers[k].history(period="1d", interval="1m", start=start, end=end)
                if not df.empty:
                    df['Ticker'] = k
                    df_list.append(df)
            except Exception as e:
                logging.error(f"Error fetching data for {k} on {start.date()}: {str(e)}")
                continue

        if df_list:
            result_df = pd.concat(df_list)
            data[k] = result_df
        else:
            logging.warning(f"No data available for {k} in the past 30 days.")

    # Push data to XCom
    return data

# Function to extract real-time data (run in subsequent executions)
def extract_real_time_data_task(tickers, **kwargs):
    logging.info("Running real-time data extraction.")
    tickers = yf.Tickers(tickers)
    data = {}

    # Fetch data for the last 5 minutes
    for k in tickers.tickers.keys():
        try:
            df = tickers.tickers[k].history(period="1d", interval="1m")
            if not df.empty:
                df['Ticker'] = k
                data[k] = df
        except Exception as e:
            logging.error(f"Error fetching real-time data for {k}: {str(e)}")
            continue

    # Push data to XCom
    return data

# Main function to decide which task to run
def conditional_data_extraction(tickers, **kwargs):
    if is_first_dag_run(**kwargs):
        # Run historical data extraction on the first run
        data = extract_historical_data_task(tickers, **kwargs)
    else:
        # Run real-time data extraction in subsequent runs
        data = extract_real_time_data_task(tickers, **kwargs)
    kwargs['ti'].xcom_push(key='extract_data', value=data)

# Function to load data into PostgreSQL (Upsert)
def load_data_to_postgres_task(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='transformed_data', key='transformed_data')
    postgres_hook = PostgresHook(postgres_conn_id='stock_connection')

    for ticker, df in data.items():
        for index, row in df.iterrows():
            
            insert_query = f"""
            INSERT INTO stock_data (ticker, date, open, high, low, close, volume, source)
            VALUES ('{row['Ticker']}', '{row['Datetime']}', {row['Open']}, {row['High']}, {row['Low']}, {row['Close']}, {row['Volume']}, 'Yahoo Finance')
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
    data = kwargs['ti'].xcom_pull(task_ids='extract_stock_data', key='extract_data')
    transformed_data = {}

    for ticker, df in data.items():
        df.reset_index(inplace=True)
        df['date'] = df['Datetime']
        transformed_data[ticker] = df

    kwargs['ti'].xcom_push(key='transformed_data', value=transformed_data)

# Define the DAG
with DAG(dag_id='yahoo_finance_etl',
         start_date=datetime(2024, 10, 20),
         schedule_interval='*/5 13-20 * * 1-5',  # Set the schedule for historical task
         catchup=False) as dag:

    # Task to create the table in PostgreSQL
    create_table_task = PostgresOperator(
        task_id='create_stock_table',
        postgres_conn_id='stock_connection',
        sql="""
        CREATE TABLE IF NOT EXISTS stock_data (
            id SERIAL PRIMARY KEY,
            ticker VARCHAR(10),
            date TIMESTAMP,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume BIGINT,
            source VARCHAR(255),
            CONSTRAINT unique_ticker_date UNIQUE (ticker, date)  -- Ensure no duplicates
        );
        """
    )

    # Task to conditionally extract data
    extract_data = PythonOperator(
        task_id='extract_stock_data',
        python_callable=conditional_data_extraction,
        op_kwargs={'tickers': ['MSFT', 'AAPL', 'GOOG', 'NVDA', 'AMZN', 'META', 'NFLX', 'TSLA']},  # List of tickers
        dag=dag
    )


    # Task to load historical data into PostgreSQL
    load_data_to_postgres = PythonOperator(
        task_id='load_data_to_postgres',
        python_callable=load_data_to_postgres_task,
        dag=dag
    )

    
    # Task to transform the data (optional)
    transform_data = PythonOperator(
        task_id='transformed_data',
        python_callable=transform_data_task
    )

    # Define task dependencies
    create_table_task >> extract_data  >> transform_data >> load_data_to_postgres


  
