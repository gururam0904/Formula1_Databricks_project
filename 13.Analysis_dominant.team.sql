-- Databricks notebook source
select * from f1_presentation.calculated_race_results


-- COMMAND ----------

select team_name,
count(*) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points,
rank() over(order by avg(calculated_points) desc) as driver_rank
 from f1_presentation.calculated_race_results
 group by team_name
 having total_races >=50
 order by avg_points desc

-- COMMAND ----------

