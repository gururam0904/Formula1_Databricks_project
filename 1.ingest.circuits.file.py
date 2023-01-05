# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuits.csv file

# COMMAND ----------

dbutils.widgets.help()


# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------


dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_data_source

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

# MAGIC %md
# MAGIC #### giving schema

# COMMAND ----------


from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_Schema=StructType(fields=[ StructField("circuitId", IntegerType(), False),
                                StructField("circuitRef",StringType(), True),
                                StructField("name", StringType(), True),
                                StructField("location", StringType(), True),
                                StructField("country",StringType(), True),
                                StructField("lat", DoubleType(), True),
                                StructField("lng", DoubleType(), True),
                                StructField("alt", IntegerType(), True),
                                StructField("url", StringType(), True) ])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read the data

# COMMAND ----------

circuits_df = spark.read\
.option("header",True)\
.schema(circuits_Schema)\
.csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### SELECT THE COLUMNS

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df=circuits_df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt"))


# COMMAND ----------

# MAGIC %md
# MAGIC #### RENAMING THE COLUMNS

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df=circuits_selected_df.withColumnRenamed("circuitId","circuit_Id")\
.withColumnRenamed("circuitRef","circuit_Ref")\
.withColumnRenamed("lat","latitude")\
.withColumnRenamed("lng","longitude")\
.withColumnRenamed("alt","altitude")\
.withColumn("data_source",lit(v_data_source))\
.withColumn("file_date",lit(v_file_date))

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### ADDING NEW COLUMN "INGESTION TIME"

# COMMAND ----------


circuits_final_df=ingestion_date(circuits_renamed_df) 

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### WRITING IN PARQUET

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits") 

# COMMAND ----------

(spark.read.parquet("/mnt/formula1dlguru/processed/circuits")).printSchema()

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dlguru/processed/circuits"))

# COMMAND ----------


dbutils.notebook.exit("success")

# COMMAND ----------

