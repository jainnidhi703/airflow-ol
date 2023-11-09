import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

# create_pet_table, populate_pet_table, get_all_pets, and get_birth_date are examples of tasks created by
# instantiating the Postgres Operator

aggregate_reporting_query = """
    INSERT INTO adoption_reporting_long (date, type, number)
    SELECT c.date, c.type, COUNT(c.type)
    FROM animal_adoptions_combined c
    GROUP BY date, type;
"""

reporting_delete_query = """
    DROP TABLE IF EXISTS adoption_reporting_long;
"""

with DAG(
    dag_id="lineage_reporting",
    start_date=datetime.datetime(2020, 2, 2),
    schedule_interval="0 0 * * *",
    catchup=False,
) as dag:
    create_table = PostgresOperator(
        task_id="create_reporting_table",
        postgres_conn_id='postgres-default',
        sql="""
            CREATE TABLE IF NOT EXISTS adoption_reporting_long (
                date DATE,
                type VARCHAR,
                number INTEGER
                );
          """,
    )
    insert_data = PostgresOperator(
        task_id='reporting',
        postgres_conn_id='postgres-default',
        sql=aggregate_reporting_query
    ) 
    delete_data =  PostgresOperator(
        task_id='delete_aggregate',
        postgres_conn_id='postgres-default',
        sql=reporting_delete_query
    ) 
    create_table >> insert_data >> delete_data