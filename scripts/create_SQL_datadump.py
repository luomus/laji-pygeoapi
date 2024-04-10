import psycopg2

# Database connection parameters
db_params = {
    'dbname': 'my_geospatial_db',
    'user': 'postgres',
    'password': 'admin123',
    'host': 'localhost',
    'port': '5432'
}

# Path to the SQL dump file
sql_dump_file = 'scripts\init_postgis_test.sql'

# Connect to the PostgreSQL database
conn = psycopg2.connect(
    dbname=db_params['dbname'],
    user=db_params['user'],
    password=db_params['password'],
    host=db_params['host'],
    port=db_params['port']
)

# Create a cursor object
cur = conn.cursor()

# Read SQL dump file and execute SQL commands
with open(sql_dump_file, 'r') as f:
    sql_commands = f.read()

cur.execute(sql_commands)

# Commit the transaction
conn.commit()

# Close the cursor and connection
cur.close()
conn.close()

print("Data imported successfully from SQL dump file.")
