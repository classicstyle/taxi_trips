import psycopg2
import psycopg2.extras
from utils import profile
from utils import execute_ddl
from utils import rows_from_a_csv_file
from utils import get_connection
from utils import json_reader
from typing import Iterator
import sys
import os


@profile
def insert_taxi_zone(
    connection,
    rows: Iterator[list],
    page_size: int = 100,
) -> None:
    with connection.cursor() as cursor:
        psycopg2.extras.execute_values(cursor, """
            INSERT INTO stage.taxi_zone VALUES %s;
        """, ((
            row[0],
            row[1],
            row[2],
            row[3],
        ) for row in rows), page_size=page_size)


def main():
    # TODO: check the existence of input file(s)
    csv_path_taxi_zone = sys.argv[1]

    dir_root = os.path.dirname(os.path.realpath(__file__))

    # config json
    config_json_path = os.path.join(dir_root, "config.json")
    config_json = json_reader(config_json_path)

    if not config_json:
        print("no config json found or empty {}".format(config_json_path))
        return 1
    else:
        if 'db' not in config_json:
            print("db config not found or empty")
            return 1

    # database settings from config
    db_settings = config_json['db']
    db_hostname = db_settings['hostname']
    db_name = db_settings['db_name']
    db_schema = db_settings['schema']
    db_username = db_settings['username']

    # TODO: better to store DDLs separately and run before this job
    ddl_taxi_zone = """
        CREATE TABLE IF NOT EXISTS {0}.taxi_zone (    
            location_id     INTEGER,
            borough         VARCHAR(255),
            zone            VARCHAR(255),
            service_zone    VARCHAR(255)
        );
    """.format(db_schema)

    # creating a connection to db
    try:
        conn = get_connection(hostname=db_hostname, db=db_name, username=db_username)
    except:
        print("Error in creating a connection.")
        return 1

    # for dev env
    conn.autocommit = True

    with conn.cursor() as cursor:
        # Create a taxi_zone table if not exists
        try:
            execute_ddl(cursor, ddl_taxi_zone)
        except:
            print("Error in creating taxi_zone table")
            # return 1

        # Insert into taxi_zone dictionary
        # TODO: insert only new rows
        try:
            insert_taxi_zone(
                conn,
                iter(rows_from_a_csv_file(csv_path_taxi_zone, skip_first_line=True)),
                page_size=1000
            )
        except:
            print("Error in inserting the data into taxi_zone table")
            return 1

    return 0


if __name__ == '__main__':

    ret = main()
    sys.exit(ret)
