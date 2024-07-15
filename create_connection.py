import sqlite3
from contextlib import contextmanager


@contextmanager
def SQLiteConnection(db_name='database.db'):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    try:
        yield cursor
        conn.commit()
    finally:
        conn.close()
