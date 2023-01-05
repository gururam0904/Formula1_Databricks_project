# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

raw_folder_path

# COMMAND ----------

# MAGIC %md
# MAGIC #### INGEST CONSTRUCTOR FILE

# COMMAND ----------

 from pyspark.sql.types import StructType, StructField, IntegerType, StringType                               
 

# COMMAND ----------

# MAGIC %md
# MAGIC #### GIVING SCHEMA

# COMMAND ----------

 constructorId_schema=StructType(fields= [StructField("constructorId", IntegerType(), False),
                                 StructField("constructorRef", StringType(), True),
                                 StructField("name", StringType(), True),
                                 StructField("nationality", StringType(),True),
                                 StructField("url", StringType(),True) ])  

# COMMAND ----------

# MAGIC %md
# MAGIC #### READING THE CONSTRUCTORS FILE

# COMMAND ----------

constructors_df=spark.read.schema(constructorId_schema)\
.json(f"{raw_folder_path}/{v_file_date}/constructors.json")


# COMMAND ----------

# MAGIC %md
# MAGIC #### DROPPING URL COLUMN

# COMMAND ----------

constructors_drop_df=constructors_df.drop(constructors_df["url"])

# COMMAND ----------

# MAGIC %md
# MAGIC #### RENAMING AND ADDING INGESTION_DATE COLUMN

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

constructors_final_df=constructors_drop_df.withColumnRenamed("constructorId","consturctor_id")\
                                           .withColumnRenamed("constructorRef","constructo_ref")\
                                           .withColumn("ingestion_date",current_timestamp())\
                                           .withColumn("data_source",lit(v_data_source))\
                                           .withColumn("file_date",lit(v_file_date))


# COMMAND ----------

# MAGIC %md
# MAGIC #### WRITING IN PROCESSED CONTAINER

# COMMAND ----------

constructors_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors") 


# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dlguru/processed/constructors"))

# COMMAND ----------


dbutils.notebook.exit("success")