# Databricks notebook source
print("Hello World!")

# COMMAND ----------

# This will give an error
SELECT "Hello world from SQL!"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "Hello world from SQL!"

# COMMAND ----------

# MAGIC %md
# MAGIC dbutils is a Databricks utility tool that provides helper methods for performing common tasks such as:
# MAGIC
# MAGIC - Managing files and directories in DBFS (Databricks File System)
# MAGIC - Handling secrets 
# MAGIC - Working with notebooks 
# MAGIC - Controlling widgets (for parameters) 
# MAGIC - Managing libraries
# MAGIC
# MAGIC

# COMMAND ----------

# List all files in default databricks workspace
dbutils.fs.ls('/databricks-datasets')

# COMMAND ----------

# Create Spark Session

# from pyspark.sql import SparkSession

# spark = SparkSession \
#     .builder \
#     .appName("word count example in apache spark") \
#     .master("local[*]") \
#     .getOrCreate()
spark

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/part_00000")

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/tables/part_00000"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Spark Core API:** The low-level API in Apache Spark, based on RDDs (Resilient Distributed Datasets), which provides fine-grained control over data processing and transformations.
# MAGIC
# MAGIC S**park High-Level API:** The user-friendly, optimized API in Spark that includes DataFrames, Datasets, and Spark SQL, allowing for easier and faster data manipulation using SQL-like or structured operations.
# MAGIC #### Example: Count total number of orders against each order_status 

# COMMAND ----------

orders_df = spark.read.csv("/FileStore/tables/part_00000", header=True, inferSchema=True)
# Define the new column names
orders_df = orders_df.toDF("id", "timestamp", "customer_id", "order_status")

# Display the first 5 rows to confirm
orders_df.display(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### RDD
# MAGIC RDD (Resilient Distributed Dataset) is the fundamental data structure in Apache Spark. It represents an immutable, distributed collection of objects that can be processed in parallel across a cluster.
# MAGIC
# MAGIC Here’s a simple breakdown:
# MAGIC
# MAGIC - Resilient: Fault-tolerant, automatically recovers from failures.
# MAGIC - Distributed: Data is split across multiple nodes in a cluster.
# MAGIC - Dataset: A collection of records (like rows in a table or lines in a file).

# COMMAND ----------

orders_rdd = orders_df.rdd
orders_rdd.take(5)

# COMMAND ----------

# Map the RDD to a tuple with (order_status, 1) for each row
status_rdd = orders_rdd.map(lambda row: (row['order_status'], 1))
status_rdd.take(5)

# COMMAND ----------

# Reduce by key to count the number of orders per status
status_counts_rdd = status_rdd.reduceByKey(lambda a, b: a + b)

# Collect the results and show them
status_counts_rdd.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Spark Dataframe
# MAGIC A Spark DataFrame is a distributed collection of data organized into named columns, similar to a table in a relational database or a DataFrame in pandas, but designed to handle large-scale data processing across clusters.

# COMMAND ----------

# Group by 'status' column and count the number of orders for each status
status_counts = orders_df.groupBy("order_status").count()

# Show the result
status_counts.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Spark SQL
# MAGIC Spark SQL is a module in Apache Spark that allows you to run SQL queries on structured and semi-structured data using the DataFrame API or pure SQL syntax.
# MAGIC
# MAGIC It combines the power of SQL with the scalability of Spark, making it easier for users familiar with SQL to work with big data.

# COMMAND ----------

# Register the DataFrame as a temporary SQL view
orders_df.createOrReplaceTempView("orders")

# Execute SQL query to group by 'order_status' and count the orders
status_counts_sql = spark.sql("""
    SELECT order_status, COUNT(*) AS order_count
    FROM orders
    GROUP BY order_status
""")

# Show the result
status_counts_sql.show()
