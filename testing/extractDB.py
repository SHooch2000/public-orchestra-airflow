import psycopg2 as db
import pandas as pd

# Connect to the database
conn_string="dbname='dataengineering' host='localhost' user='postgres' password='password'"
conn = db.connect(conn_string)

# Execute a select statement
df = pd.read_sql("select * from people", conn)

# Print the first 5 rows
print(df.head())

