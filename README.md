## NYC Taxi Trip Analysis
This solution was implemented to analyze NYC Taxi Trips by ingesting the data into Postgres database using Python, and the pipeline orchestrated in Apache Airflow.

![High Level Picture](https://github.com/classicstyle/taxi_trips/blob/master/img/arch.png?raw=true)

### Technologies used
* Python
* Postgres
* Apache Airflow

### Installing
Python dependencies:
```
pip install psycopg2-binary
pip install memory_profiler
```

Postgres requirements:
* Create a db (taxi_trips).
* All DDL scripts to create schemas and tables are located here: <code>airflow/include/db_structure_ddl.sql</code>.

Airflow dependencies:
```
from airflow.providers.postgres.operators.postgres import PostgresOperator
```

### How to run the pipeline
There are two DAGs in airflow/dag folder: <br>
* <code>ingestion_pipeline_dag</code> transfers the data from csv to the stage area of Postgres. <br>
* <code>metrics_dag</code> calculates the metrics (for tasks 2.1, 2.2) based on data from stage, and inserts the data into "metrics" schema.

In <code>airflow/include</code> folder you will find SQL queries that calculates trends.

Manually starting python jobs:
```
python3 load_dictionaries.py /path/to/csv_file
python3 load_taxi_trips.py /path/to/csv_file
```

### Thoughts for the future

* <i>Docker image</i> - to include all mentioned techs (Python, Postgres, Airflow) - so everything can be up in one click.
* Add feature to ingestion part: 
  - row validation 
  - handling different schemas of csv files
  - logging
* <i>Columnar engine</i> for Postgres in order to make analytical queries faster.
* Write more tests


