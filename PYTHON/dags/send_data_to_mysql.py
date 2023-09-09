import mysql.connector
import csv
from datetime import datetime

# MySQL connection settings
mysql_config = {
    "host": "127.0.0.1",  # Use the IP address of your MySQL server
    "port": 3306,         # MySQL default port
    "user": "root",
    "password": "password",
}

# Create a connection to MySQL
conn = mysql.connector.connect(**mysql_config)

# Create a new database if it doesn't exist
new_database_name = "ad"
cursor = conn.cursor()
cursor.execute(f"CREATE DATABASE IF NOT EXISTS {new_database_name}")

# Define the table name and its schema based on df_schema
table_name = "adv"
df_schema = [
    '_id',
    'Daily_Time_Spent_on_Site',
    'Age',
    'Area_Income',
    'Daily_Internet_Usage',
    'Ad_Topic_Line',
    'City',
    'Male',
    'Country',
    'Timestamp',
    'Clicked_on_Ad'
]
cursor.execute(f"USE {new_database_name}")

# Create the table if it doesn't exist
create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ("
create_table_query += ", ".join([f"`{col.replace(' ', '_')}` VARCHAR(255)" for col in df_schema])
create_table_query += ")"
cursor.execute(create_table_query)

# Load CSV data from a file
input_file_path = r"/opt/airflow/output.csv"
with open(input_file_path, 'r') as file:
    csv_data = csv.DictReader(file)

# Assuming your CSV data is a list of dictionaries where each dictionary represents a row
for record in csv_data:
    timestamp_str = datetime.strptime(record['Timestamp'], '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
    record['Timestamp'] = timestamp_str

    columns = ', '.join([col.replace(' ', '_') for col in df_schema])
    placeholders = ', '.join(['%s'] * len(record))
    values = list(record.values())

    cursor.execute(f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})", values)

print("Database updated")

# Commit the changes and close the connection
conn.commit()
cursor.close()
conn.close()
