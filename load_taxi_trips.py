import psycopg2
import psycopg2.extras
import sys
from typing import Iterator
from utils import profile
from utils import execute_ddl
from utils import rows_from_a_csv_file
from utils import str_to_decimal
from utils import str_to_timestamp
from utils import get_connection


@profile
def insert_taxi_trips(
    connection,
    rows: Iterator[list],
    page_size: int = 100,
) -> None:
    with connection.cursor() as cursor:
        psycopg2.extras.execute_values(cursor, """
            INSERT INTO stage.taxi_trips VALUES %s;
        """, ((
            row[0],
            str_to_timestamp(row[1]),
            str_to_timestamp(row[2]),
            row[3],
            row[4],
            row[5],
            row[6],
            row[7],
            str_to_decimal(row[8]),
            str_to_decimal(row[9]),
            str_to_decimal(row[10]),
            str_to_decimal(row[11]),
            str_to_decimal(row[12]),
            str_to_decimal(row[13]),
            str_to_decimal(row[14]),
            str_to_decimal(row[15]),
            str_to_decimal(row[16]),
            row[17],
            row[18],
            str_to_decimal(row[19]),
            "2",
        ) for row in rows), page_size=page_size)


def main():
    # TODO: add logging

    # TODO: check the existence of input file(s)
    # TODO: read multiple csv files or call this script multiple times with given path to csv
    csv_path_taxi_trips = sys.argv[1]

    # TODO: put in a config file, include db configs
    db_schema = "stage"

    ddl_taxi_trips = """
            CREATE TABLE IF NOT EXISTS {0}.taxi_trips (    
                vendor_id               INTEGER,
                lpep_pickup_datetime    TIMESTAMP,
                lpep_dropoff_datetime   TIMESTAMP,
                store_and_fwd_flag      VARCHAR(1),
                ratecode_id             INTEGER,
                pulocation_id           INTEGER,
                dolocation_id           INTEGER,
                passenger_count         INTEGER,
                trip_distance           DECIMAL,
                fare_amount             DECIMAL,
                extra                   DECIMAL,
                mta_tax                 DECIMAL,
                tip_amount              DECIMAL,
                tolls_amount            DECIMAL,
                ehail_fee               DECIMAL,
                improvement_surcharge   DECIMAL,
                total_amount            DECIMAL,
                payment_type            INTEGER,
                trip_type               INTEGER,
                congestion_surcharge    DECIMAL,
                taxi_type               VARCHAR(50)
            );
        """.format(db_schema)

    try:
        conn = get_connection(hostname="localhost", db="taxi_trips", username="postgres")
    except:
        print("Error in creating a connection.")
        return 1

    # for dev env
    conn.autocommit = True

    with conn.cursor() as cursor:
        # Create a taxi_trips table if not exists
        try:
            execute_ddl(cursor, ddl_taxi_trips)
        except:
            print("Error in creating taxi_trips table")
            # return 1

        # Insert the rows from taxi trips csv by reading using generator
        # TODO: insert only new rows (taxi_type, yyyy-mm)
        try:
            insert_taxi_trips(
                conn,
                iter(rows_from_a_csv_file(csv_path_taxi_trips, skip_first_line=True)),
                page_size=1000
            )
        except:
            print("Error in inserting the data into taxi_trips table")
            return 1

    return 0


if __name__ == '__main__':

    ret = main()
    sys.exit(ret)
