import psycopg2
from psycopg2 import Error

try:
    # Connect to an existing database
    connection = None
    connection = psycopg2.connect(
        user="bemidb",
        password="bemidb",
        host="127.0.0.1",
        port="5432",
        database="tpch"
    )

    print("Connected")
    cursor = connection.cursor()

    print("Querying")
    # cursor.execute("SELECT 1")
    cursor.execute("SET datestyle TO 'ISO'")
    record = cursor.fetchone()
    print("You are connected to - ", record)

except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL:", error)

finally:
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")
