# Databricks notebook source
# MAGIC %run "/Users/shanmukh@shanmukhsattiraju.com/04. Common"

# COMMAND ----------

dbutils.widgets.text(name="env",defaultValue='',label='Enter the environment in lower case')
env = dbutils.widgets.get("env")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Creating a read_Traffic_Data() Function

# COMMAND ----------

def read_Traffic_Data():
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
    from pyspark.sql.functions import current_timestamp
    print("Reading the Raw Traffic Data :  ", end='')
    schema = StructType([
    StructField("Record_ID",IntegerType()),
    StructField("Count_point_id",IntegerType()),
    StructField("Direction_of_travel",StringType()),
    StructField("Year",IntegerType()),
    StructField("Count_date",StringType()),
    StructField("hour",IntegerType()),
    StructField("Region_id",IntegerType()),
    StructField("Region_name",StringType()),
    StructField("Local_authority_name",StringType()),
    StructField("Road_name",StringType()),
    StructField("Road_Category_ID",IntegerType()),
    StructField("Start_junction_road_name",StringType()),
    StructField("End_junction_road_name",StringType()),
    StructField("Latitude",DoubleType()),
    StructField("Longitude",DoubleType()),
    StructField("Link_length_km",DoubleType()),
    StructField("Pedal_cycles",IntegerType()),
    StructField("Two_wheeled_motor_vehicles",IntegerType()),
    StructField("Cars_and_taxis",IntegerType()),
    StructField("Buses_and_coaches",IntegerType()),
    StructField("LGV_Type",IntegerType()),
    StructField("HGV_Type",IntegerType()),
    StructField("EV_Car",IntegerType()),
    StructField("EV_Bike",IntegerType())
    ])

    rawTraffic_stream = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format","csv")
        .option('cloudFiles.schemaLocation',f'{checkpoint}/rawTrafficLoad/schemaInfer')
        .option('header','true')
        .schema(schema)
        .load(landing+'/raw_traffic/')
        .withColumn("Extract_Time", current_timestamp()))
    
    print('Reading Succcess !!')
    print('*******************')

    return rawTraffic_stream

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Creating read_Road_Data() Function

# COMMAND ----------

def read_Road_Data():
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
    from pyspark.sql.functions import current_timestamp
    print("Reading the Raw Roads Data :  ", end='')
    schema = StructType([
        StructField('Road_ID',IntegerType()),
        StructField('Road_Category_Id',IntegerType()),
        StructField('Road_Category',StringType()),
        StructField('Region_ID',IntegerType()),
        StructField('Region_Name',StringType()),
        StructField('Total_Link_Length_Km',DoubleType()),
        StructField('Total_Link_Length_Miles',DoubleType()),
        StructField('All_Motor_Vehicles',DoubleType())
        
        ])

    rawRoads_stream = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format","csv")
        .option('cloudFiles.schemaLocation',f'{checkpoint}/rawRoadsLoad/schemaInfer')
        .option('header','true')
        .schema(schema)
        .load(landing+'/raw_roads/')
        )
    
    print('Reading Succcess !!')
    print('*******************')

    return rawRoads_stream

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Creating write_Traffic_Data(StreamingDF,environment) Function

# COMMAND ----------

def write_Traffic_Data(StreamingDF,environment):
    print(f'Writing data to {environment}_catalog raw_traffic table', end='' )
    write_Stream = (StreamingDF.writeStream
                    .format('delta')
                    .option("checkpointLocation",checkpoint + '/rawTrafficLoad/Checkpt')
                    .outputMode('append')
                    .queryName('rawTrafficWriteStream')
                    .trigger(availableNow=True)
                    .toTable(f"`{environment}_catalog`.`bronze`.`raw_traffic`"))
    
    write_Stream.awaitTermination()
    print('Write Success')
    print("****************************")    

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Creating write_Road_Data(StreamingDF,environment) Function

# COMMAND ----------

def write_Road_Data(StreamingDF,environment):
    print(f'Writing data to {environment}_catalog raw_roads table', end='' )
    write_Data = (StreamingDF.writeStream
                    .format('delta')
                    .option("checkpointLocation",checkpoint + '/rawRoadsLoad/Checkpt')
                    .outputMode('append')
                    .queryName('rawRoadsWriteStream')
                    .trigger(availableNow=True)
                    .toTable(f"`{environment}_catalog`.`bronze`.`raw_roads`"))
    
    write_Data.awaitTermination()
    print('Write Success')
    print("****************************")    

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Calling read and Write Functions

# COMMAND ----------

## Reading the raw_traffic's data from landing to Bronze
read_Df = read_Traffic_Data()

## Reading the raw_roads's data from landing to Bronze
read_roads = read_Road_Data()

## Writing the raw_traffic's data from landing to Bronze
write_Traffic_Data(read_Df,env)

## Writing the raw_roads's data from landing to Bronze
write_Road_Data(read_roads,env)

# COMMAND ----------

