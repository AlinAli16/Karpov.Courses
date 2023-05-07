import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

def get_data(): # Getting the data
    
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

def top_10_domain_zone(): # Top 10 domain zones by domain's quantity
    top_doms_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_10_domain_zone_df = top_doms_df['domain'].apply(lambda x: x.split('.')[-1]).value_counts().to_frame().reset_index().head(10)
    top_10_domain_zone_df.columns = ['domain_zone', 'quantity']
    
    with open ('top_10_domain_zone_df.csv', 'w') as f:
        f.write(top_10_domain_zone_df.to_csv(index=False, header=False))
        
def max_length_domain(): # Domain with the longest name (the first one in alphabetical order if several)
    top_doms_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_doms_df['domain_len'] = top_doms_df['domain'].apply(lambda x: len(x))
    max_length_domain_str = top_doms_df.sort_values(['domain_len', 'domain'],ascending=[False, True]).reset_index().domain[0]
    
    with open('max_length_domain_str.txt', 'w') as f:
        f.write(str(max_length_domain_str))

def airflow_rank(): # The airflow domain's rank
    top_doms_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    try:
        air_rank = int(top_doms_df.loc[top_doms_df['domain'] == 'airflow.com', 'rank'])
    except:
        air_sim = (top_doms_df.loc[top_doms_df['domain'].str.startswith('airflow')][['domain', 'rank']]
                   .reset_index(drop=True))
        air_rank = f'There is no "airflow.com" but similar domains are the following:{dict(air_sim.values)}'
    
    with open('air_rank.txt', 'w') as f:
        f.write(str(air_rank))
    
def print_data(ds): # Printing the results
    with open('top_10_domain_zone_df.csv', 'r') as f:
        top_10_domain_zone_df = f.read()
    with open('max_length_domain_str.txt', 'r') as f:
        max_length_domain_str = f.read()
    with open('air_rank.txt', 'r') as f:
        air_rank = f.read()
        
    date = ds

    print(f'Top 10 domain zones for date: {date}')
    print(top_10_domain_zone_df)
    
    print(f'Domain with the longest name for date: {date}')
    print(max_length_domain_str)
    
    print(f'Airflow rank for date: {date}')
    print(air_rank)

default_args = {
    'owner': 'a-petuhova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 14),
    'schedule_interval': '0 14 * * *'
}
dag = DAG('a-petuhova_lesson2_hw', default_args=default_args)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_top10 = PythonOperator(task_id='top_10_domain_zone',
                    python_callable=top_10_domain_zone,
                    dag=dag)

t2_max_len = PythonOperator(task_id='max_length_domain',
                    python_callable=max_length_domain,
                    dag=dag)

t2_air_rank = PythonOperator(task_id='airflow_rank',
                    python_callable=airflow_rank,
                    dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)


t1 >> [t2_top10, t2_max_len, t2_air_rank] >> t3