import os
import json
import tweepy
import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from political_twitter_trends.scripts.tweets_to_bq import get_tweets_upload_to_bq, create_gcp_conn


# create gcp connection
create_gcp_conn('my_gcp_conn', 
               'twitter-pipeline-269020',
               '/usr/local/airflow/dags/support/keys/twitter-pipeline-269020-e5bcffa09de2.json' )

### set variables required for twitter scraping
# keys_dict for twitter view access
keys_dict = {
      'key':       '4rFQghkGKjSkK2SuSTSFa46mT',
      'secret_key':     'OJfiCIMYQleB0fls87UllGSLJPPrXKnkJeNxbIxrbbgMj9mOPd'
            }
# accounts of users to scrape
users = ['ewarren', 'berniesanders', 'petebuttigieg', 'joebiden', 'amyklobuchar']

# result limit of single api call
result_limit = 200 

# specify project and bq destination to upload tweets
project_id = "twitter-pipeline-269020"
table_id = 'stg.tweets'

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'wait_for_downstream': True,    
    'start_date': datetime.datetime(2020, 2, 19),
    'email': ['airflow@airflow.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': datetime.timedelta(seconds=15)
}

dag = DAG(
    'bigquery_political_twitter_trends', 
    default_args=default_args, 
    schedule_interval='@daily'
    )

# Task 0: scrape twitter timelines of users specified above; load to bq
t0 = PythonOperator(
    task_id='py_get_tweets_upload_to_stg', 
    python_callable=get_tweets_upload_to_bq,
    provide_context=True,
    op_kwargs={'users': users, 
               'min_date': None, # passed via context as yesterday_ds
               'max_date': None,
               'result_limit': result_limit,
               'key': keys_dict['key'],
               'secret_key': keys_dict['secret_key'],
               'project_id': project_id,
               'table_id': table_id},
    dag=dag
    )

## Task 1: check tweets loaded to bq stg table
t1 = BigQueryCheckOperator(
        task_id='bq_check_stg_tweets_day',
        sql='sql/check_stg_tweets.sql',
        use_legacy_sql=False,
        bigquery_conn_id='my_gcp_conn',
        dag=dag
    )
## Task 2: check table schema exists for tweets in prod table
t2 = BigQueryCheckOperator(
        task_id='bq_check_for_prod_tbl',
        sql='sql/check_prod_tweets.sql',
        use_legacy_sql=False,
        bigquery_conn_id='my_gcp_conn',
        dag=dag
    )
## Task 3: insert latest tweets from stg table into prod table
t3 = BigQueryOperator(
        task_id='insert_stg_tweets_into_prod',    
        sql='sql/stg_tweets_to_prod.sql',
        write_disposition='WRITE_APPEND',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id='my_gcp_conn',
        dag=dag
    )
## Task 4: aggregate tweets and create dashboard table
t4 = BigQueryOperator(
        task_id='bq_create_prod_agg_tbl',    
        sql='sql/create_prod_agg_tbl.sql',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id='my_gcp_conn',
        dag=dag
    )
  
t0 >> t1 >> t2 >> t3 >> t4
