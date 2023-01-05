# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------


dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------


dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC #### INGESTI DRIVERS FILE

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.COMBINE TWO COL OF NAME COL TO A SINGLE COL AND GIVING SCHEMA

# COMMAND ----------


from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

name_schema=StructType(fields=[StructField("forename", StringType(), True),
                                 StructField("surname", StringType(), True)])  

# COMMAND ----------

drivers_schema=StructType(fields= [StructField("driverId", IntegerType(), False),
                                 StructField("driverRef", StringType(), True),
                                 StructField("name", name_schema),
                                 StructField("nationality", StringType(),True),
                                 StructField("url", StringType(),True),
                                 StructField("code", StringType(), True),
                                 StructField("dob", DateType(),True),
                                 StructField(" number", IntegerType(),True)]) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### READING THE FILE

# COMMAND ----------

drivers_df=spark.read\
.schema(drivers_schema)\
.json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### RENAMING AND ADD COLUMN AND CONCAT THE NAME SUB COLUMNS

# COMMAND ----------


from pyspark.sql.functions import concat,current_timestamp, lit ,col

# COMMAND ----------

drivers_rename_df=drivers_df.withColumnRenamed("driverId","driver_id")\
                            .withColumnRenamed("driverRef","driver_ref")\
                            .withColumn("ingestion_date",current_timestamp())\
                            .withColumn("name",concat(col("name.forename"),lit(" "),col("name.surname")))\
                            .withColumn("data_source",lit(v_data_source))\
                            .withColumn("file_date",lit(v_file_date))



# COMMAND ----------

# MAGIC %md
# MAGIC #### DROPPING URL COLUMN

# COMMAND ----------

drivers_final_df=drivers_rename_df.drop(drivers_rename_df["url"])

# COMMAND ----------

# MAGIC %md
# MAGIC #### WRITING IN PROCESSED CONTAINER

# COMMAND ----------

  drivers_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.drivers") 

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dlguru/processed/drivers"))

# COMMAND ----------


dbutils.notebook.exit("success")