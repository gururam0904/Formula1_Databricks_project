# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

circuits_df=spark.read.parquet(f"{processed_folder_path}/circuits")\
.withColumnRenamed("location","circuit_location")\
.withColumnRenamed("circuit_Id","circuit_id")

# COMMAND ----------

drivers_df=spark.read.parquet(f"{processed_folder_path}/drivers")\
.withColumnRenamed("name", "driver_name")\
.withColumnRenamed("nationality","driver_nationality")\
.withColumnRenamed("number","driver_number")


# COMMAND ----------

constructors_df=spark.read.parquet(f"{processed_folder_path}/constructors")\
.withColumnRenamed("name","team")\
.withColumnRenamed("consturctor_id","constructor_id")

# COMMAND ----------

races_df=spark.read.parquet(f"{processed_folder_path}/races")\
.withColumnRenamed("name","race_name")\
.withColumnRenamed("race_timestamp","race_date")\
.withColumnRenamed("circuit_Id","circuit_id")\
.withColumnRenamed("race_Id","race_id")

# COMMAND ----------

results_df=spark.read.parquet(f"{processed_folder_path}/results")\
.withColumnRenamed("time","race_time")

# COMMAND ----------

race_circuits_df=races_df.join(circuits_df,races_df.circuit_id==circuits_df.circuit_id)\
.select(races_df.race_id,races_df.race_year,races_df.race_name,races_df.race_date,circuits_df.circuit_location)

# COMMAND ----------

race_results_df=results_df.join(race_circuits_df,results_df.race_id==race_circuits_df.race_id)\
                          .join(drivers_df,results_df.driver_id==drivers_df.driver_id)\
                          .join(constructors_df,results_df.constructor_id==constructors_df.constructor_id)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

last_df=race_results_df.select("race_year","race_name","race_date","circuit_location","driver_name","number","driver_nationality","team","grid",
                               "fastest_lap","race_time","points","position")\
                       .withColumn("create_date",current_timestamp())       

# COMMAND ----------

final_df=last_df.withColumnRenamed("number","driver_number")

# COMMAND ----------



final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")

# COMMAND ----------

