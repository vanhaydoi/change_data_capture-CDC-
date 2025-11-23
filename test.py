# Import necessary SparkSession from pyspark.sql
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# 1. Initialize SparkSession
# SparkSession is the entry point to programming Spark with the DataFrame API.
# .builder: Returns a SparkSession.Builder that can be used to configure and create the SparkSession.
# .appName("DataFrameCreation"): Sets a name for the Spark application, which will be shown in the Spark UI.
# .getOrCreate(): Gets an existing SparkSession or, if there is no existing one, creates a new one based on the options set in this builder.
spark = SparkSession.builder \
    .appName("DataFrameCreation") \
    .getOrCreate()

# 2. Create a DataFrame from a list of tuples (common method)
# Define the data as a list of tuples. Each tuple represents a row.
data = [
    ("Alice", 1, "New York"),
    ("Bob", 2, "Los Angeles"),
    ("Charlie", 3, "Chicago"),
    ("David", 4, "Houston")
]

# Define the schema for the DataFrame.
# StructType: Represents a schema.
# StructField: Represents a field in a StructType, specifying name, data type, and nullability.
schema = StructType([
    StructField("Name", StringType(), True),
    StructField("ID", IntegerType(), True),
    StructField("City", StringType(), True)
])

# Create the DataFrame using spark.createDataFrame()
# The data and schema are passed as arguments.
df_from_list = spark.createDataFrame(data=data, schema=schema)

# Show the DataFrame and its schema
print("task 1 start")
print("DataFrame created from a list of tuples:")
df_from_list.show()
df_from_list.printSchema()
print("task 1 end")
# 3. Create a DataFrame from a list of dictionaries (another common method)
# Define the data as a list of dictionaries. Keys become column names.
data_dict = [
    {"Name": "Eve", "Age": 25, "Occupation": "Engineer"},
    {"Name": "Frank", "Age": 30, "Occupation": "Doctor"},
    {"Name": "Grace", "Age": 28, "Occupation": "Artist"}
]

# When creating from dictionaries, Spark can infer the schema,
# but it's often better to define it explicitly for robustness.
schema_dict = StructType([
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Occupation", StringType(), True)
])

df_from_dict = spark.createDataFrame(data=data_dict, schema=schema_dict)

# Show the DataFrame and its schema
print("task 2 start")
print("\nDataFrame created from a list of dictionaries:")
df_from_dict.show()
df_from_dict.printSchema()
print("task 2 end")

# 4. Create an empty DataFrame
# You can create an empty DataFrame with a predefined schema.
empty_df = spark.createDataFrame([], schema) # Reusing the 'schema' defined earlier

print("\nEmpty DataFrame:")
empty_df.show()
empty_df.printSchema()

# 5. Stop the SparkSession
# It's good practice to stop the SparkSession when you are done with it
# to release resources.
spark.stop()