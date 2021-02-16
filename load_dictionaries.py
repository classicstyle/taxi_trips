import psycopg2
import psycopg2.extras
from utils import profile
from utils import execute_ddl
from utils import rows_from_a_csv_file
from utils import get_connection
from typing import Iterator
import sys


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

    # TODO: put in a config file, include db configs
    db_schema = "stage"

    ddl_taxi_zone = """
        CREATE TABLE IF NOT EXISTS {0}.taxi_zone (    
            location_id     INTEGER,
            borough         VARCHAR(255),
            zone            VARCHAR(255),
            service_zone    VARCHAR(255)
        );
    """.format(db_schema)

    # TODO: create a function for this
    try:
        conn = get_connection(hostname="localhost", db="taxi_trips", username="yeldos")
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
