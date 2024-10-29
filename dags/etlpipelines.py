from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook


import json

with DAG (
    dag_id="nasa_aprod_postgres",
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    @task
    def create_table():
        postgres_hook=PostgresHook(postgres_conn_id="my_postgres_connection")

        create_table_query="""
                        CREATE TABLE IF NOT EXIST apod_data(
                        id SERIAL PRIMARY_KEY,
                        title VARCHAR(255),
                        explanation TEXT,
                        url TEXT,
                        Date DATE,
                        media_type VARCHAR(50)

                        );

                        """ 
        postgres_hook.run(create_table_query)

    extract_apod=SimpleHttpOperator(
            task_id="Extract_apod",
            http_conn_id="nasa_api",
            endpoint='planetary/apod',
            method="GET",
            data={"api_key":"{{conn_nasa_api.extra_dejson.api_key}}"},
            response_filter=lambda response:response.json(),
        )
    

    @task
    def transform_apod_data(response):
        apod_data={
            "title":response.get('title',""),
            "explanation":response.get('explanation',""),
            "url":response.get('url',""),
            "Date":response.get('Date',""),
            "media_type":response.get('media_type',""),


        }
        return apod_data
    
    @task
    def load_data_to_postgres(apod_data):
        postgres_hook=PostgresHook(postgres_conn_id="my_postgres_connection")

        insert_query="""
            INSERT INTO apod_data(title,explanation,url,Date,media_type) VALUES(%s,%s,%s,%s,%s)
"""
        postgres_hook.run(insert_query,parameters=(
            apod_data['title'],
            apod_data['explanation'],
            apod_data['url'],
            apod_data['Date'],
            apod_data['media_type']

        ))


    
    create_table()>>extract_apod
    api_response=extract_apod.output
    transformed_data=transform_apod_data(api_response)
    load_data_to_postgres(transformed_data) 