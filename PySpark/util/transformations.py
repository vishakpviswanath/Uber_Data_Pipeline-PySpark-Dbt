# Undefined name `try_to_date` fixed by replacing with `to_date`

from typing import List
from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable


class Transformations:

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

                    
    
    