#!/usr/bin/env python3

import os
import signal
import time

import psycopg2


MINUTES=60

DEBUG=1 if os.environ.get('DEBUG') else 0

def open_db(dbname):
    if DEBUG > 0:
        print(f"Connecting: postgresql://{os.environ['POSTGRES_USER']}@{os.environ['POSTGRES_HOST']}/{dbname}")
    return psycopg2.connect(user=os.environ["POSTGRES_USER"],
                            password=os.environ["POSTGRES_PWD"],
                            host=os.environ["POSTGRES_HOST"],
                            database=dbname)

def write_db(conn, table, **kwargs):
    if DEBUG > 0:
        print(f"Writing to {table}:\n{kwargs}\n")

    cursor = conn.cursor()

    names = []
    values = ()
    for k, v in kwargs.items():
        names  += [k]
        values += (v,)

    query = f" INSERT INTO {table} ({','.join(names)}) VALUES ({','.join(['%s'] * len(values))}) ; "
    cursor.execute(query, values)

def close_db(conn):
    conn.commit()
    conn.close()

def main():
    running = True

    def sig_handler(_sign, _stf):
        running = False

    signal.signal(signal.SIGINT,  sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGABRT, sig_handler)

    conn = None
    while running:
        try: 
            conn = open_db("hellos")
            write_db(conn, 
                     "hellos",
                     timestamp=time.ctime(),
                     msg="Hello, World!")
            close_db(conn)
            time.sleep(5*MINUTES)
        except InterruptedError:
            pass
    if conn is not None:
        close_db(conn)

if __name__ == "__main__":
    main()

