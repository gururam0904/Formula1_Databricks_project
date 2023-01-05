-- Databricks notebook source
select * from f1_presentation.calculated_race_results

-- COMMAND ----------

select * from f1_presentation.calculated_race_results

-- COMMAND ----------

select driver_name,
count(*) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points
 from f1_presentation.calculated_race_results
 where race_year between 2011 and 2020 
 group By driver_name 
  having total_races >= 50
 order by avg_points desc