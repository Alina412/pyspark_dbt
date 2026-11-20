from typing import List
from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql.functions import concat, row_number, col

class transformations:
    def deduplication(self, df: DataFrame, dedup_cols:list, cdc:str): 
        df = df.withColumn("dedup_key", concat(*dedup_cols))
        df = df.withColumn("dedup_counts", row_number().over(Window.partitionBy("dedup_key").orderBy(col(cdc).desc())))
        df = df.filter(col("dedup_counts") == 1)
        df = df.drop("dedup_key", "dedup_counts")
        return df

  ##  def rename_columns(self, df, column)
