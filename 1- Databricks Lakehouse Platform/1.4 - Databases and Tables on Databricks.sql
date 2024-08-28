-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Managed Tables

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pyspark.pandas as ps
-- MAGIC from pyspark.sql.functions import * 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC %pip install openpyxl
-- MAGIC
-- MAGIC import pandas as pd
-- MAGIC
-- MAGIC df = pd.read_excel("/Volumes/certswokspace/analysis/files/Planilha Excel TR PE 1-2020.xlsx", header=0)
-- MAGIC spark_df = spark.createDataFrame(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Use %pip to install pandas if not already installed
-- MAGIC %pip install pandas
-- MAGIC
-- MAGIC import pandas as pd
-- MAGIC
-- MAGIC df = pd.read_excel(
-- MAGIC     "/Volumes/certswokspace/analysis/files/Planilha Excel TR PE 1-2020.xlsx",
-- MAGIC     header=0
-- MAGIC )
-- MAGIC
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %pip install openpyxl

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pandas as pd
-- MAGIC df = pd.read_excel("/Volumes/certswokspace/analysis/files/Planilha Excel TR PE 1-2020.xlsx", header=1)
-- MAGIC display(df)

-- COMMAND ----------

USE CATALOG hive_metastore;

CREATE TABLE managed_default
  (width INT, length INT, height INT);

INSERT INTO managed_default
VALUES (3 INT, 2 INT, 1 INT)

-- COMMAND ----------

create volume meu volume 


-- COMMAND ----------

select * from temp_view_phones_brands

-- COMMAND ----------

CREATE TABLE celular_v2 (id string, nome string, marca string, ano string)

-- COMMAND ----------

describe table celular_v2

-- COMMAND ----------

describe table celular

-- COMMAND ----------

CREATE TABLE CELULAR AS
SELECT * FROM global_temp.global_temp_view_latest_phones

-- COMMAND ----------

SELECT * FROM global_temp.global_temp_view_latest_phones

-- COMMAND ----------

select * from global_temp_view_latest_phones

-- COMMAND ----------

DESCRIBE EXTENDED managed_default

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## External Tables

-- COMMAND ----------

CREATE TABLE external_default
  (width INT, length INT, height INT)
LOCATION 'dbfs:/mnt/demo/external_default';
  
INSERT INTO external_default
VALUES (3 INT, 2 INT, 1 INT)

-- COMMAND ----------

DESCRIBE EXTENDED external_default

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Dropping Tables

-- COMMAND ----------

DROP TABLE managed_default

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/managed_default'

-- COMMAND ----------

DROP TABLE external_default

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo/external_default'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating Schemas

-- COMMAND ----------

CREATE SCHEMA new_default

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED new_default

-- COMMAND ----------

USE new_default;

CREATE TABLE managed_new_default
  (width INT, length INT, height INT);
  
INSERT INTO managed_new_default
VALUES (3 INT, 2 INT, 1 INT);

-----------------------------------

CREATE TABLE external_new_default
  (width INT, length INT, height INT)
LOCATION 'dbfs:/mnt/demo/external_new_default';
  
INSERT INTO external_new_default
VALUES (3 INT, 2 INT, 1 INT);

-- COMMAND ----------

DESCRIBE EXTENDED managed_new_default

-- COMMAND ----------

DESCRIBE EXTENDED external_new_default

-- COMMAND ----------

DROP TABLE managed_new_default;
DROP TABLE external_new_default;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/new_default.db/managed_new_default'

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo/external_new_default'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating Schemas in Custom Location

-- COMMAND ----------

CREATE SCHEMA custom
LOCATION 'dbfs:/Shared/schemas/custom.db'

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED custom

-- COMMAND ----------

USE custom;

CREATE TABLE managed_custom
  (width INT, length INT, height INT);
  
INSERT INTO managed_custom
VALUES (3 INT, 2 INT, 1 INT);

-----------------------------------

CREATE TABLE external_custom
  (width INT, length INT, height INT)
LOCATION 'dbfs:/mnt/demo/external_custom';
  
INSERT INTO external_custom
VALUES (3 INT, 2 INT, 1 INT);

-- COMMAND ----------

DESCRIBE EXTENDED managed_custom

-- COMMAND ----------

DESCRIBE EXTENDED external_custom

-- COMMAND ----------

DROP TABLE managed_custom;
DROP TABLE external_custom;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/Shared/schemas/custom.db/managed_custom'

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo/external_custom'
