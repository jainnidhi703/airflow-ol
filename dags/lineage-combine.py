import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

create_base_tables_query = """
CREATE TABLE IF NOT EXISTS adoption_center_1
(date DATE, type VARCHAR, name VARCHAR, age INTEGER);

CREATE TABLE IF NOT EXISTS adoption_center_2
(date DATE, type VARCHAR, name VARCHAR, age INTEGER);

INSERT INTO
    adoption_center_1 (date, type, name, age)
VALUES
    ('2022-01-01', 'Dog', 'Bingo', 4),
    ('2022-02-02', 'Cat', 'Bob', 7),
    ('2022-03-04', 'Fish', 'Bubbles', 2);

INSERT INTO
    adoption_center_2 (date, type, name, age)
VALUES
    ('2022-06-10', 'Horse', 'Seabiscuit', 4),
    ('2022-07-15', 'Snake', 'Stripes', 8),
    ('2022-08-07', 'Rabbit', 'Hops', 3);
"""

with DAG(
    dag_id="lineage_combine",
    start_date=datetime.datetime(2020, 2, 2),
    schedule_interval="0 0 * * *",
    catchup=False,
) as dag:
    create_base_table = PostgresOperator(
        task_id="create_base_table",
        postgres_conn_id='postgres-default',
        sql=create_base_tables_query,
    )
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
    create_base_table >> create_table >> insert_data