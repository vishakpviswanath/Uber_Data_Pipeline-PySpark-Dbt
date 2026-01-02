# Databricks notebook source
# MAGIC %md
# MAGIC ###Silver Layer Transformation

# COMMAND ----------

import sys
import os
sys.path.append(os.path.abspath('..'))
from util.transformations import *

from pyspark.sql.functions import *

# COMMAND ----------

############ TEMP CODE######################
from typing import List
from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

################## TEMP CODE ################
class Transformations_temp:

    def type_convert(self, df: DataFrame, column: str, trg_type: str) -> DataFrame:
        if trg_type == 'date':
            df = df.withColumn(column, to_date(col(column), 'yyyy-MM-dd'))
            return df
        elif trg_type == 'timestamp':
            df = df.withColumn(column, to_timestamp(col(column), 'yyyy-MM-dd HH:mm:ss'))
            return df
        elif trg_type == 'int':
            df = df.withColumn(column, col(column).cast('int'))
            return df
        elif trg_type == 'float':
            df = df.withColumn(column, col(column).cast('decimal(9,2)'))
            return df
        elif trg_type == 'decimal':
            df = df.withColumn(column, col(column).cast('decimal(9,6)'))
            return df
        else:
            raise ValueError(f"Invalid target type: {trg_type}")

    def deduplicate(self, df: DataFrame, column: List, cdc: str) -> DataFrame:
        
        df = df.withColumn('dedupKey',concat(*column)) 
        df = df.withColumn('dedupCounts', row_number().over(Window.partitionBy('dedupKey').orderBy(desc(cdc))))
        df = df.filter(df.dedupCounts == 1).drop('dedupKey','dedupCounts','_rescued_data')
        return df
    
    def domain_lookup(self, df: DataFrame, column: str) -> DataFrame:
        df = df.withColumn('domain', split(col(column), '@')[1])
        return df
    def phone_cleanup(self, df: DataFrame, column: str) -> DataFrame:
        df = df.withColumn(column, regexp_replace(col(column), r"[^0-9]", ""))
        return df

    def process_timestamp(self, df: DataFrame) -> DataFrame:
        df = df.withColumn('process_timestamp', current_timestamp())
        return df
    
    def upsert(self, df, table, key, cdc):

        key_con = ' AND '.join([f"trg.{key} = src.{key}" for key in key])

        dlt_obj = DeltaTable.forName(spark,f'pyspark_dbt.silver.{table}').alias("trg").merge(
                    df.alias("src"), key_con)\
                    .whenMatchedUpdateAll(condition=f"src.{cdc} >= trg.{cdc}")\
                    .whenNotMatchedInsertAll()\
                    .execute()
        return 1

                    

# COMMAND ----------

trans_obj = Transformations_temp()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Processing Customers Table

# COMMAND ----------

df = spark.read.table("pyspark_dbt.bronze.bronze_customers")

# COMMAND ----------

df = trans_obj.type_convert(df,'signup_date','date')
df = trans_obj.type_convert(df,'last_updated_timestamp','timestamp')
df = trans_obj.type_convert(df,'customer_id','int')

df = trans_obj.deduplicate(df,['customer_id'],'last_updated_timestamp')

df = trans_obj.phone_cleanup(df,'phone_number')
df = trans_obj.domain_lookup(df,'email')
df = trans_obj.process_timestamp(df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### UPSERT Customers

# COMMAND ----------

if not(spark.catalog.tableExists("pyspark_dbt.silver.silver_customers")):
    df.write.format("delta")\
        .mode("append")\
        .saveAsTable("pyspark_dbt.silver.silver_customers")
else:
    trans_obj.upsert(df,'silver_customers',['customer_id'],'last_updated_timestamp')
    

# COMMAND ----------

# MAGIC %md
# MAGIC ###Processing Drivers

# COMMAND ----------

df_drivers = spark.read.table("pyspark_dbt.bronze.bronze_drivers")

# COMMAND ----------

df_drivers = trans_obj.type_convert(df_drivers,'last_updated_timestamp','timestamp')
df_drivers = trans_obj.type_convert(df_drivers,'driver_id','int')
df_drivers = trans_obj.type_convert(df_drivers,'driver_rating','float')
df_drivers = trans_obj.type_convert(df_drivers,'vehicle_id','int')

df_drivers = trans_obj.deduplicate(df_drivers,['driver_id'],'last_updated_timestamp')

df_drivers = trans_obj.phone_cleanup(df_drivers,'phone_number')
df_drivers = trans_obj.process_timestamp(df_drivers)

# COMMAND ----------

# MAGIC %md
# MAGIC ### UPSERT DRIVERS

# COMMAND ----------

if not(spark.catalog.tableExists("pyspark_dbt.silver.silver_drivers")):
    df_drivers.write.format("delta")\
        .mode("append")\
        .saveAsTable("pyspark_dbt.silver.silver_drivers")
else:
    trans_obj.upsert(df_drivers,'silver_drivers',['driver_id'],'last_updated_timestamp')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Processing Locations

# COMMAND ----------

df_loc = spark.read.table("pyspark_dbt.bronze.bronze_locations")

# COMMAND ----------

df_loc = trans_obj.type_convert(df_loc,'last_updated_timestamp','timestamp')
df_loc = trans_obj.type_convert(df_loc,'location_id','int')
df_loc = trans_obj.type_convert(df_loc,'latitude','decimal')
df_loc = trans_obj.type_convert(df_loc,'longitude','decimal')

df_loc = trans_obj.deduplicate(df_loc,['location_id'],'last_updated_timestamp')
df_loc = trans_obj.process_timestamp(df_loc)


# COMMAND ----------

# MAGIC %md
# MAGIC ### UPSERT LOCATION

# COMMAND ----------

if not(spark.catalog.tableExists("pyspark_dbt.silver.silver_locations")):
    df_loc.write.format("delta")\
        .mode("append")\
        .saveAsTable("pyspark_dbt.silver.silver_locations")
else:
    trans_obj.upsert(df_loc,'silver_locations',['location_id'],'last_updated_timestamp')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Processing Payments

# COMMAND ----------

df_pay = spark.read.table("pyspark_dbt.bronze.bronze_payments")


# COMMAND ----------

df_pay = trans_obj.type_convert(df_pay,'last_updated_timestamp','timestamp')
df_pay = trans_obj.type_convert(df_pay,'payment_id','int')
df_pay = trans_obj.type_convert(df_pay,'amount','float')
df_pay = trans_obj.type_convert(df_pay,'transaction_time','timestamp')
df_pay = trans_obj.type_convert(df_pay,'trip_id','int')
df_pay = trans_obj.type_convert(df_pay,'customer_id','int')

df_pay = trans_obj.deduplicate(df_pay,['payment_id'],'last_updated_timestamp')
df_pay = trans_obj.process_timestamp(df_pay)


# COMMAND ----------

# MAGIC %md
# MAGIC ### UPSERT Payments

# COMMAND ----------

if not(spark.catalog.tableExists("pyspark_dbt.silver.silver_payments")):
    df_pay.write.format("delta")\
        .mode("append")\
        .saveAsTable("pyspark_dbt.silver.silver_payments")
else:
    trans_obj.upsert(df_pay,'silver_payments',['payment_id'],'last_updated_timestamp')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Processing Vehicles

# COMMAND ----------

df_vec = spark.read.table("pyspark_dbt.bronze.bronze_vehicles")

# COMMAND ----------

df_vec = trans_obj.type_convert(df_vec,'last_updated_timestamp','timestamp')
df_vec = trans_obj.type_convert(df_vec,'vehicle_id','int')
df_vec = trans_obj.type_convert(df_vec,'year','int')

df_vec = trans_obj.deduplicate(df_vec,['vehicle_id'],'last_updated_timestamp')
df_vec = trans_obj.process_timestamp(df_vec)


# COMMAND ----------

# MAGIC %md
# MAGIC ### UPSERT Vehicles

# COMMAND ----------

if not(spark.catalog.tableExists("pyspark_dbt.silver.silver_vehicles")):
    df_vec.write.format("delta")\
        .mode("append")\
        .saveAsTable("pyspark_dbt.silver.silver_vehicles")
else:
    trans_obj.upsert(df_vec,'silver_vehicles',['vehicle_id'],'last_updated_timestamp')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from pyspark_dbt.silver.silver_trips

# COMMAND ----------

