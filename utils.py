

def profile(fn):
    import time
    from functools import wraps
    from memory_profiler import memory_usage

    @wraps(fn)
    def inner(*args, **kwargs):
        fn_kwargs_str = ', '.join(f'{k}={v}' for k, v in kwargs.items())
        print(f'\n{fn.__name__}({fn_kwargs_str})')

        # Measure time
        t = time.perf_counter()
        retval = fn(*args, **kwargs)
        elapsed = time.perf_counter() - t
        print(f'Time   {elapsed:0.4}')

        # Measure memory
        mem, retval = memory_usage((fn, args, kwargs), retval=True, timeout=200, interval=1e-7)

        print(f'Memory {max(mem) - min(mem)}')
        return retval

    return inner


@profile
def execute_ddl(cursor, ddl_string) -> None:
    cursor.execute(ddl_string)


@profile
def rows_from_a_csv_file(filename, skip_first_line=False, dialect='excel', **fmtparams):
    """
    function reads CSV file using generator to minimize memory usage
    """
    import csv

    with open(filename, 'r', encoding='utf-8') as csv_file:
        reader = csv.reader(csv_file, dialect, **fmtparams)
        if skip_first_line:
            next(reader, None)
        for row in reader:
            # type(row) -> list
            # TODO: validate the row
            yield row


def str_to_timestamp(s):
    """Converts string to timestamp."""
    import datetime

    return datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S")


def str_to_decimal(s):
    """Converts string to decimal."""
    from decimal import Decimal
    from decimal import getcontext

    getcontext().prec = 2
    res = Decimal(0)
    try:
        res = Decimal(s)
    except:
        pass

    return res


def get_connection(hostname, db, username, password=None):
    import psycopg2

    return psycopg2.connect(
            host=hostname,
            database=db,
            user=username,
            password=password,
        )


def json_reader(json_file):
    import json

    data = {}
    with open(json_file) as data_file:
        data = json.load(data_file)

    return data
