import psycopg2
from psycopg2 import Error

try:
    # Connect to an existing database
    connection = None
    connection = psycopg2.connect(
        user="bemidb",
        password="bemidb",
        host="127.0.0.1",
        port="54321",
        database="bemidb"
    )

    print("Connected")
    cursor = connection.cursor()

    print("Querying")
    cursor.execute("SELECT 1")
    record = cursor.fetchone()
    print("You are connected to - ", record)

except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL:", error)

finally:
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")
