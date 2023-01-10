# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("csv").getOrCreate()




# COMMAND ----------

# read csv file
df_csv = spark.read.option("header","True").csv("dbfs:/FileStore/tables/sample2__1_.csv",inferSchema=True)
display(df_csv)
print("total number of rows",df_csv.count())



# COMMAND ----------

df2 = df_csv.withColumnRenamed("Name","Name") \
    .withColumnRenamed("'Team'","Team").withColumnRenamed("'Position'","position").withColumnRenamed("'Height(inches)'","height").withColumnRenamed("'Weight(lbs)'","weight").withColumnRenamed("'Age'","age")
display(df2)

# COMMAND ----------

df2.printSchema()

# COMMAND ----------

