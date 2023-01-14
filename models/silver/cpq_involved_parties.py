from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *

def InvolvedParties_df(df):
    return df.withColumn("InvolvedParties", explode_outer(col("InvolvedParties"))).select("QuoteId","InvolvedParties.*")

def model(dbt, session):

    dbt.config(materialized="table")
    dbt.config(schema="cpq")

    df = session.read.json("/mnt/cntdlt/bronze/cpq/file_1/")

    return InvolvedParties_df(df)