from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
from functools import reduce
from pyspark.sql.functions import lit, col

def model(dbt, session):

    dbt.config(materialized="table")
    dbt.config(schema="cpq")

    df = session.read.json("/mnt/cntdlt/bronze/cpq/file_1/")

    l1 = ['AverageGrossMarginPercent', 'AverageProductDiscountPercent', 'TotalAmount', 'TotalListPrice', 'TotalNetPrice', 'TotalProductDiscountAmount']
    
    list_of_dfs = []
    for i in l1:
        df1 = df.select("QuoteId", i + ".*")
        v = df1.columns
        for j in range(1, len(v)):
            list_of_dfs.append(df1.withColumnRenamed(v[j], i + "_" + v[j]))  

    df_result = reduce(lambda left, right: left.join(right, on=["QuoteId"]), list_of_dfs)
    df_result = df_result.drop("Value", "Currency")

    return df_result