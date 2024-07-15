from create_connection import SQLiteConnection


def create_table():
    with SQLiteConnection() as cursor:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS UKTrade (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Cn8Code BIGINT,
            Cn8LongDescription TEXT,
            "EU/NonEU" TEXT,
            Continent TEXT,
            Country TEXT,
            PortName TEXT,
            "Value (£)" NUMERIC(15, 1),
            "NetMass (Kg)" NUMERIC(12, 1),
            SuppUnit NUMERIC(12, 1),
            FlowType TEXT,
            Year INT,
            Month TEXT
        );
        """)

if __name__ == '__main__':
    create_table()
