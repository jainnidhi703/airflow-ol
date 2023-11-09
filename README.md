# Airflow OpenLineage DockerFile
Airflow OpenLineage DockerFile and Sample DAGs

## Run

```bash
 docker-compose up
```

This will start the following containers:
- Redis
- Postgres
- Airflow Scheduler
- Airflow Triggerer
- Airfloe Worker
- Airflow WebServer

## Requirements

These DAGS need a Postgres connection in Airflow WebServer with configuration:
- Connection ID: postgres-default
- Connection Type: Postgres
- Host: host.docker.internal
- Schema: airflow
- Login: airflow
- Port: 5432

## DAGS

There are 2 DAGS lineage_combine and lineage_reporting, with Postgres Operator. 
- lineage_reporting: This dag has 3 tasks create_base__table, create_table and combine. 

