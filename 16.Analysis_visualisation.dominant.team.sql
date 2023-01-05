-- Databricks notebook source
-- MAGIC %python
-- MAGIC html="""<h1 style="color:Red;text-align:center;font-familt:Ariel">Report on Formula 1 Dominant Teams </h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

create or replace temp view v_viz_dom_teams
as 
select team_name,
count(*) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points,
rank() over(order by avg(calculated_points) desc) as team_rank
 from f1_presentation.calculated_race_results
 group By team_name 
  having total_races >= 50
 order by avg_points desc

-- COMMAND ----------


select team_name,race_year,
count(*) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points
 from f1_presentation.calculated_race_results
 where team_name in (select team_name from  v_viz_dom_teams where team_rank <= 5)
 group By team_name ,race_year
 order by race_year, avg_points desc

-- COMMAND ----------

