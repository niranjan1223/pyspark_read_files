# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.functions import *

spark=SparkSession.builder.master("local[*]").appName("json").getOrCreate()



# COMMAND ----------

# read json file
df = spark.read.option("multiline","True").json("dbfs:/FileStore/tables/sample_json.json")
df.show(truncate=False)
df.printSchema()




# COMMAND ----------

# flattern data -->explode()
expl=df.withColumn("pipil",explode("people")).drop("people")
expl.show(truncate=False)


df_final = df.withColumn("pipil",explode("people")).withColumn("AGE",col("people.age")).\
    withColumn("FIRSTNAME",col("people.firstName")).withColumn("LASTNAME",col("people.lastName")).\
    withColumn("GENDER",col("people.gender")).withColumn("CONTACT",col("people.number")).drop("people","pipil")
df_final.show(truncate=False)
df_final.printSchema()

df_age=df_final.withColumn("result_age",explode("AGE")).withColumn('result_firstname',explode("FIRSTNAME")).\
    withColumn('result_lastname',explode("LASTNAME")).withColumn('result_gen',explode('GENDER')).\
    withColumn("res_contact",explode("CONTACT")).drop("AGE","FIRSTNAME","LASTNAME","GENDER","CONTACT")

df_age.show(truncate=False)


# COMMAND ----------

