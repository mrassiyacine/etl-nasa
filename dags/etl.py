from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.operators.http import HttpOperator
from airflow.decorators import task
from datetime import datetime, timedelta

with DAG(
    dag_id= 'NASA_ETL',
    start_date= datetime.today() - timedelta(days=1),
    schedule='@daily',
    catchup=False,
) as dag:
    @task
    def create_table():
        pg_hook = PostgresHook(postgres_conn_id='my_postgres_connection')
        create_table_query = """
        CREATE TABLE IF NOT EXISTS apod_data(
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url VARCHAR(255),
            date DATE,
            media_type VARCHAR(50)
            );
        """
        pg_hook.run(create_table_query)


    extract_apod = HttpOperator(
        task_id ='extract_apod',
        http_conn_id = 'nasa_api',
        endpoint = 'planetary/apod',
        method = 'GET',
        data = {"api_key": "{{ conn.nasa_api.extra_dejson.api_key }}"},
        response_filter =lambda response: response.json(),
    )
    @task
    def transform_apod_data(response):
        apod_data = {
            'title': response.get('title',''),
            'explanation': response.get('explanation',''),
            'url': response.get('url',''),
            'date': response.get('date',''),
            'media_type': response.get('media_type',''),
        }
        return apod_data
    @task
    def load_data_postgres(apod_data):
        pg_hook = PostgresHook(postgres_conn_id='my_postgres_connection')
        insert_query = """
        INSERT INTO apod_data (title, explanation, url, date, media_type)
        VALUES (%s, %s, %s, %s, %s);
        """
        pg_hook.run(insert_query, parameters=(apod_data['title'], apod_data['explanation'], apod_data['url'], apod_data['date'], apod_data['media_type']))
    
    create_table() >> extract_apod
    api_response = extract_apod.output
    transformed_data = transform_apod_data(api_response)
    load_data_postgres(transformed_data)