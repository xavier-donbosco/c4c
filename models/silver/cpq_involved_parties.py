from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
from pyspark.sql.functions import lit, col

def InvolvedParties_df(df):
    df = df.withColumn("InvolvedParties", explode_outer(col("InvolvedParties"))).select("QuoteId","InvolvedParties.*")
    return df

def model(dbt, session):

    dbt.config(materialized="table")
    dbt.config(schema="cpq")

    df = session.read.json("/mnt/cntdlt/bronze/cpq/file_1/")

    df = InvolvedParties_df(df)

    return df