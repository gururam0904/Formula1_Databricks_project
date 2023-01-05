# Databricks notebook source
dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

v_data_source

# COMMAND ----------

# MAGIC %md
# MAGIC #### INGEST PIT_SHOPS FILE

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. GIVING SCHEMA

# COMMAND ----------


from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

qualifying_schema=StructType(fields= [StructField("raceId", IntegerType(),True),
                                 StructField("qualifyId", IntegerType(), True),
                                 StructField("driverId", IntegerType(), True),
                                 StructField("constructorId", IntegerType(), True),
                                 StructField("number", IntegerType(),True),
                                 StructField("position", IntegerType(), True),
                                 StructField("q2", StringType(),True),
                                 StructField("q3", StringType(), True),
                                 StructField("q1", StringType(), True)])

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. READING THE FILE

# COMMAND ----------

qualifying_df=spark.read.schema(qualifying_schema)\
.option("multiLine",True)\
.json('dbfs:/mnt/formula1dlguru/raw/qualifying')

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. ADDING AND RENAMING COLUMN

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------


qualifying_renamed_df=qualifying_df.withColumnRenamed("raceId","race_id")\
                                   .withColumnRenamed("driverId","driver_id")\
                                   .withColumnRenamed("driverId","driver_id")\
                                   .withColumn("ingestion_date",current_timestamp()).withColumn("data_source",lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. WRITING IN PROCESSED CONTAINER

# COMMAND ----------


qualifying_renamed_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------



# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dlguru/processed/qualifying"))

# COMMAND ----------


dbutils.notebook.exit("success")