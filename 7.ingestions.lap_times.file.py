# Databricks notebook source
dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC #### INGEST LAP_TIMES_FOLDER

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. GIVING SCHEMA

# COMMAND ----------


from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------


lap_times_schema=StructType(fields= [StructField("raceId", IntegerType(),True),
                                 StructField("position", IntegerType(), True),
                                 StructField("driverId", IntegerType(), True),
                                 StructField("lap", IntegerType(), True),
                                 StructField("time", StringType(),True),
                                 StructField("milliseconds", IntegerType(), True)])

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. READING THE FILE

# COMMAND ----------

lap_times_df=spark.read.schema(lap_times_schema)\
.csv('dbfs:/mnt/formula1dlguru/raw/lap_times')

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. ADDING AND RENAMING COLUMN

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------


lap_times_renamed_df=lap_times_df.withColumnRenamed("driverId", "driver_id") \
                                 .withColumnRenamed("raceId", "race_id") \
                                 .withColumn("ingestion_date", current_timestamp()).withColumn("data_source",lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. WRITING IN PROCESSED CONTAINER

# COMMAND ----------

lap_times_renamed_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times") 

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dlguru/processed/lap_times"))

# COMMAND ----------


dbutils.notebook.exit("success")