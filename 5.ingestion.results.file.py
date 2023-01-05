# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------


dbutils.widgets.text("p_file_date","2021-03-28")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC #### INGEST RESULTS.JSON

# COMMAND ----------

# MAGIC %md
# MAGIC #### GIVING SCHEMA

# COMMAND ----------


 from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType   

# COMMAND ----------

results_schema=StructType(fields= [StructField("constructorId", IntegerType(), True),
                                 StructField("driverId", IntegerType(), True),
                                 StructField("fastestLap",IntegerType(), True),
                                 StructField("fastestLapSpeed", StringType(), True),
                                 StructField("fastestLapTime", StringType(),True),
                                 StructField("grid", IntegerType(),True) ,
                                 StructField("laps", IntegerType(), True),
                                 StructField("milliseconds",  IntegerType(), True),
                                 StructField("number",  IntegerType(), True),
                                 StructField("points", FloatType(),True),
                                 StructField("position",  IntegerType(),True),
                                 StructField("positionOrder", IntegerType(), True),
                                 StructField("positionText", StringType(), True),
                                 StructField("raceId", IntegerType(),True),
                                 StructField("rank", StringType(), True),
                                 StructField("resultId", IntegerType(),False),
                                 StructField("statusId", StringType(),True),
                                 StructField("time", StringType(),True)]) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### READING

# COMMAND ----------

results_df=spark.read\
.schema(results_schema)\
.json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### ADDING COLUMN AND RENAMING COLUMN

# COMMAND ----------


from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

results_with_columns_df = results_df.withColumnRenamed("resultId", "result_id")\
                                    .withColumnRenamed("raceId", "race_id")\
                                    .withColumnRenamed("driverId", "driver_id")\
                                    .withColumnRenamed("constructorId", "constructor_id")\
                                    .withColumnRenamed("positionText", "position_text")\
                                    .withColumnRenamed("positionOrder", "position_order")\
                                    .withColumnRenamed("fastestLapTime", "fastest_lap_time")\
                                    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")\
                                    .withColumnRenamed("fastestLap", "fastest_lap")\
                                    .withColumn("ingestion_date", current_timestamp()).withColumn("data_source",lit(v_data_source))\
                                    .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### DROPING STATUSID COLUMN

# COMMAND ----------

results_final_df=results_with_columns_df.drop(results_with_columns_df["statusId"])

# COMMAND ----------

# MAGIC %md
# MAGIC #### WRITING IN PROCESSED CONTAINER

# COMMAND ----------

results_final_df.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results") 

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id,count(*)
# MAGIC from f1_processed.results
# MAGIC group by race_id
# MAGIC order by race_id desc

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dlguru/processed/results"))

# COMMAND ----------


dbutils.notebook.exit("success")