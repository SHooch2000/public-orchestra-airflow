import psycopg2 as db
from faker import Faker

# Connect to the database
conn_string="dbname='dataengineering' host='localhost' user='postgres' password='password'"
conn = db.connect(conn_string)

# Open a cursor to perform database operations
cur = conn.cursor()

# Initialize Faker and store data, id starting at 2
fake = Faker()
data = [(x,fake.name(),fake.street_address(),fake.city(),fake.postcode()) for x in range(2,1000)]

#Convert data to tuple
data_for_db=tuple(data)

# Execute a command: this inserts a row into the people table
query = "insert into people (id,name,street,city,postcode) values(%s,%s,%s,%s,%s)"

# Pass data to fill a query placeholders and let Psycopg perform
cur.executemany(query,data_for_db)

# Make the changes to the database persistent
conn.commit()