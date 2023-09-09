from pyspark.sql import *
from pyspark.sql.functions import *

# Create a Spark session
spark = SparkSession.builder \
    .appName("LocalDataToPySpark") \
    .getOrCreate()

# Read a CSV file
df = spark.read.csv("E:\\git\\PYTHON\\dags\\project\\output.csv", header=True, inferSchema=True)

# Perform operations on the DataFrame (e.g., df.show(), df.select(), df.groupBy())

# Show the DataFrame contents
df.show()

# Stop the Spark session

