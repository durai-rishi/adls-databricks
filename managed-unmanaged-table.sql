-- Databricks notebook source
-- DBTITLE 1,create managed table

create table if not exists demo_emp(
  id int,
  name string
) using delta;

insert into demo_emp values(1,"ram"),(2,"raj")




-- COMMAND ----------

-- DBTITLE 1,create external table

create table if not exists emp_demo_ext (
  id int,
  name string
)using delta
location "/ext_tables/demo_emp";

insert into emp_demo_ext values(1,"ram"),(2,"raj")

-- COMMAND ----------


--select * from demo_emp

select * from emp_demo_ext


-- COMMAND ----------


--describe history demo_emp
describe history emp_demo_ext

-- COMMAND ----------


--drop table demo_emp
drop table emp_demo_ext


-- COMMAND ----------



select * from delta.`/ext_tables/demo_emp`



-- COMMAND ----------

-- MAGIC %scala
-- MAGIC
-- MAGIC dbutils.fs.rm("/ext_tables",true)
