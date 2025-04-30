-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## Creating a source Table

-- COMMAND ----------

CREATE TABLE Source_Table
(
    Education_Level VARCHAR(50),
    Line_Number INT,
    Employed INT,
    Unemployed INT,
    Industry VARCHAR(50),
    Gender VARCHAR(10),
    Date_Inserted DATE,
    dense_rank INT
)

-- COMMAND ----------

INSERT INTO Source_Table
VALUES
    ('Bachelor', 100, 4500, 500, 'Networking', 'Male', '2023-07-12', 1),
    ('Master', 101, 6500, 1500, 'Networking', 'Female', '2023-07-12', 2),
    ('Master', 103, 5500, 500, 'Networking', 'Female', '2023-07-12', 3);

-- COMMAND ----------

SELECT * FROM source_table


-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Creating destination table

-- COMMAND ----------

CREATE TABLE `delta`.Dest_Table
(
    Education_Level VARCHAR(50),
    Line_Number INT,
    Employed INT,
    Unemployed INT,
    Industry VARCHAR(50),
    Gender VARCHAR(10),
    Date_Inserted DATE,
    dense_rank INT
)

-- COMMAND ----------

INSERT INTO delta.Dest_Table
VALUES
    ('Bachelor', 100, 1500, 1500, 'Networking', 'Male', '2023-07-12', 1),
    ('Master', 101, 2500, 2000, 'Networking', 'Female', '2023-07-12', 2);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Applying UPSERT using MERGE

-- COMMAND ----------

MERGE INTO `delta`.Dest_Table AS Dest
USING Source_Table as Source
    on Dest.Line_Number = Source.Line_Number
  WHEN MATCHED
    THEN UPDATE SET
  Dest.Education_Level = Source.Education_Level,
  Dest.Line_Number = Source.Line_Number,
  Dest.Employed = Source.Employed,
  Dest.Unemployed = Source.Unemployed,
  Dest.Industry = Source.Industry,
  Dest.Gender = Source.Gender,
  Dest.Date_Inserted = Source.Date_Inserted,
  Dest.dense_rank = Source.dense_rank

  WHEN NOT MATCHED
  THEN INSERT
    (Education_Level, Line_Number, Employed, Unemployed, Industry, Gender, Date_Inserted, dense_rank)
    VALUES(Source.Education_Level, Source.Line_Number, Source.Employed, Source.Unemployed, Source.Industry, Source.Gender, Source.Date_Inserted, Source.dense_rank)

-- COMMAND ----------

SELECT * FROM `delta`.Dest_Table 

-- COMMAND ----------


