from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
from pyspark.sql.functions import lit

def customfields_df(df):
    df = df.withColumn("CustomFields", explode_outer(col("CustomFields"))).select("QuoteId" ,"CustomFields.*")
    return df.groupBy("QuoteId").pivot("Name").agg(first("Content"))


def model(dbt, session):

    dbt.config(materialized="table")
    dbt.config(schema="cpq")

    df = session.read.json("/mnt/cntdlt/bronze/cpq/file_1/")

    df = customfields_df(df)
    df = df.withColumnRenamed("Customer's Language", "Customer Language")
    lis = df.columns
    lis1 = []
    for i in lis:
        lis1.append(i.replace(" ", "_" ))
    for i in range(len(lis)):
        df = df.withColumnRenamed(lis[i], lis1[i])
    load_date = dbt.config.get("load_date")
    df = df.withColumn("load_date", lit(load_date))
    return df