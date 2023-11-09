import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

# create_pet_table, populate_pet_table, get_all_pets, and get_birth_date are examples of tasks created by
# instantiating the Postgres Operator

with DAG(
    dag_id="lineage_combine",
    start_date=datetime.datetime(2020, 2, 2),
    schedule_interval="0 0 * * *",
    catchup=False,
) as dag:
    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id='postgres-default',
        sql="""
            CREATE TABLE IF NOT EXISTS animal_adoptions_combined (
        date DATE,
        type VARCHAR,
        name VARCHAR,
        age INTEGER
        );
          """,
    )
    insert_data = PostgresOperator(
        task_id='combine',
        postgres_conn_id='postgres-default',
        sql="""
        INSERT INTO animal_adoptions_combined (date, type, name, age) 
        SELECT * 
        FROM adoption_center_1
        UNION 
        SELECT *
        FROM adoption_center_2;
        """
    ) 
    create_table >> insert_data