from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
from functools import reduce
from pyspark.sql.functions import lit, col

def model(dbt, session):

    dbt.config(materialized="table")
    dbt.config(schema="cpq")

    df = spark.read.json("/mnt/cntdlt/bronze/cpq/file_2/")
    lis = ['CurrencyCode', 'DateCreated', 'DateModified', 'DistributionChannel', 'Division', 'EffectiveDate', 'ErrorMessage', 'ExternalId', 'ExternalSystemId', 'IsPrimary', 'MarketCode', 'MarketId', 'OpportunityId', 'OpportunityName', 'Origin', 'OwnerId', 'PriceBookId', 'QuoteId', 'QuoteNumber', 'RevisionNumber', 'StatusId', 'StatusName']
    df = df.select(* lis)

    return df