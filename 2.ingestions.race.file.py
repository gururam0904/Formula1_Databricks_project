# Databricks notebook source
# MAGIC %md
# MAGIC #### INGEST RACES.CSV

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

raw_folder_path

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC #### GIVING SCHEMA

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

races_schema=StructType(fields= [StructField("raceId", IntegerType(), True),
                                 StructField("year", IntegerType(), True),
                                 StructField("round", IntegerType(), True),
                                 StructField("circuitId", IntegerType(), True),
                                 StructField("name", StringType(), True),
                                 StructField("date", DateType(), True),
                                 StructField("time", StringType(),True) ])

# COMMAND ----------

# MAGIC %md
# MAGIC #### reading CSV file

# COMMAND ----------

races_df=spark.read.option("header",True)\
.schema(races_schema)\
.csv(f"{raw_folder_path}/{v_file_date}/races.csv")


# COMMAND ----------

# MAGIC %md
# MAGIC #### adding new column and converting date and time to a single column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, col, lit, concat

# COMMAND ----------

races_with_timestamp_df=races_df.withColumn("race_timestamp",to_timestamp(concat(col("date"),lit(" "),col("time")), "yyyy-MM-dd HH:mm:ss"))\
.withColumn("ingestion_date",current_timestamp())\
.withColumn("data_source",lit(v_data_source))\
.withColumn("file_date",lit(v_file_date))
                                



# COMMAND ----------

# MAGIC %md
# MAGIC #### selecting the wanted column

# COMMAND ----------

races_selected_df= races_with_timestamp_df.select( col("raceId").alias("race_Id"), col("year").alias("race_year") \
                 , col("round"), col("circuitId").alias("circuit_Id"), col("name"), col("ingestion_date")\
                 ,col("race_timestamp"),col("data_source"),col("file_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### writing in processed container

# COMMAND ----------


races_selected_df.write.mode("overwrite").partitionBy("race_year").format("parquet").saveAsTable("f1_processed.races") 

# COMMAND ----------


display(spark.read.parquet("/mnt/formula1dlguru/processed/races"))

# COMMAND ----------


dbutils.notebook.exit("success")