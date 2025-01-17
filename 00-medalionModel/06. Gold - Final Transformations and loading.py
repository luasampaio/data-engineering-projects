# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Calling common notebook to re-use variables

# COMMAND ----------

# MAGIC %run "/Users/shanmukh@shanmukhsattiraju.com/04. Common"

# COMMAND ----------

dbutils.widgets.text(name="env",defaultValue='',label='Enter the environment in lower case')
env = dbutils.widgets.get("env")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Read Silver_Traffic table

# COMMAND ----------


def read_SilverTrafficTable(environment):
    print('Reading the Silver Traffic Table Data : ',end='')
    df_SilverTraffic = (spark.readStream
                    .table(f"`{environment}_catalog`.`silver`.silver_traffic")
                    )
    print(f'Reading {environment}_catalog.silver.silver_traffic Success!')
    print("**********************************")
    return df_SilverTraffic

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Read silver_roads Table

# COMMAND ----------


def read_SilverRoadsTable(environment):
    print('Reading the Silver Table Silver_roads Data : ',end='')
    df_SilverRoads = (spark.readStream
                    .table(f"`{environment}_catalog`.`silver`.silver_roads")
                    )
    print(f'Reading {environment}_catalog.silver.silver_roads Success!')
    print("**********************************")
    return df_SilverRoads

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Creating vehicle Intensity Column

# COMMAND ----------

def create_VehicleIntensity(df):
 from pyspark.sql.functions import col
 print('Creating Vehicle Intensity column : ',end='')
 df_veh = df.withColumn('Vehicle_Intensity',
               col('Motor_Vehicles_Count') / col('Link_length_km')
               )
 print("Success!!!")
 print('***************')
 return df_veh

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating LoadTime column

# COMMAND ----------

def create_LoadTime(df):
    from pyspark.sql.functions import current_timestamp
    print('Creating Load Time column : ',end='')
    df_timestamp = df.withColumn('Load_Time',
                      current_timestamp()
                      )
    print('Success!!')
    print('**************')
    return df_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Writing Data to Gold Traffic
# MAGIC

# COMMAND ----------

def write_Traffic_GoldTable(StreamingDF,environment):
    print('Writing the gold_traffic Data : ',end='') 

    write_gold_traffic = (StreamingDF.writeStream
                .format('delta')
                .option('checkpointLocation',checkpoint+ "GoldTrafficLoad/Checkpt/")
                .outputMode('append')
                .queryName("GoldTrafficWriteStream")
                .trigger(availableNow=True)
                .toTable(f"`{environment}_catalog`.`gold`.`gold_traffic`"))
    
    write_gold_traffic.awaitTermination()
    print(f'Writing `{environment}_catalog`.`gold`.`gold_traffic` Success!')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Writing Data to Gold Roads
# MAGIC

# COMMAND ----------

def write_Roads_GoldTable(StreamingDF,environment):
    print('Writing the gold_roads Data : ',end='') 

    write_gold_roads = (StreamingDF.writeStream
                .format('delta')
                .option('checkpointLocation',checkpoint+ "GoldRoadsLoad/Checkpt/")
                .outputMode('append')
                .queryName("GoldRoadsWriteStream")
                .trigger(availableNow=True)
                .toTable(f"`{environment}_catalog`.`gold`.`gold_roads`"))
    
    write_gold_roads.awaitTermination()
    print(f'Writing `{environment}_catalog`.`gold`.`gold_roads` Success!')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Calling all functions
# MAGIC

# COMMAND ----------

## Reading from Silver tables
df_SilverTraffic = read_SilverTrafficTable(env)
df_SilverRoads = read_SilverRoadsTable(env)
    
## Tranformations     
df_vehicle = create_VehicleIntensity(df_SilverTraffic)
df_FinalTraffic = create_LoadTime(df_vehicle)
df_FinalRoads = create_LoadTime(df_SilverRoads)


## Writing to gold tables    
write_Traffic_GoldTable(df_FinalTraffic,env)
write_Roads_GoldTable(df_FinalRoads,env)

# COMMAND ----------

