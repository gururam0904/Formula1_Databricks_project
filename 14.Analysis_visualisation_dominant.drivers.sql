-- Databricks notebook source
-- MAGIC %python
-- MAGIC html="""<h1 style="color:Red;text-align:center;font-familt:Ariel">Report on Formula 1 Dominant Drivers </h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

create or replace temp view v_viz_dom_drivers
as 
select driver_name,
count(*) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points,
rank() over(order by avg(calculated_points) desc) as driver_rank
 from f1_presentation.calculated_race_results
 group By driver_name 
  having total_races >= 50
 order by avg_points desc

-- COMMAND ----------


select driver_name,race_year,
count(*) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points
 from f1_presentation.calculated_race_results
 where driver_name in (select driver_name from  v_viz_dom_drivers where driver_rank <= 10)
 group By driver_name ,race_year
 order by race_year, avg_points desc

-- COMMAND ----------

