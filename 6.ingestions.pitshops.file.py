# Databricks notebook source
dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC #### INGEST PIT_SHOPS FILE

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. GIVING SCHEMA

# COMMAND ----------


from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pit_shops_schema=StructType(fields= [StructField("raceId", IntegerType(),True),
                                 StructField("stop", StringType(), True),
                                 StructField("driverId", IntegerType(), True),
                                 StructField("lap", IntegerType(), True),
                                 StructField("time", StringType(),True),
                                 StructField("duration", StringType(), True),
                                 StructField("milliseconds", IntegerType(), True)])

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. READING THE FILE

# COMMAND ----------

pit_shops_df=spark.read.schema(pit_shops_schema)\
.option("multiLine",True)\
.json('dbfs:/mnt/formula1dlguru/raw/pit_stops.json')

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. ADDING AND RENAMING COLUMN

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------


pit_shops_renamed_df=pit_shops_df.withColumnRenamed("driverId", "driver_id") \
                                 .withColumnRenamed("raceId", "race_id") \
                                 .withColumn("ingestion_date", current_timestamp()).withColumn("data_source",lit(v_data_source))
 

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. WRITING IN PROCESSED CONTAINER

# COMMAND ----------

pit_shops_renamed_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops") 

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dlguru/processed/pit_stops"))

# COMMAND ----------


dbutils.notebook.exit("success")

# COMMAND ----------

