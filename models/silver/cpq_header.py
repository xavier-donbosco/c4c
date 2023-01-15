from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
from pyspark.sql.functions import lit, col

def header_df(df, list_of_col):
    df = df.select(* list_of_col)
    return df

def model(dbt, session):

    dbt.config(materialized="table")
    dbt.config(schema="cpq")

    df = session.read.json("/mnt/cntdlt/bronze/cpq/file_1/")

    list_of_header_cols = ['DateModified', 'DistributionChannel', 'Division', 'EffectiveDate', 'ErrorMessage', 'ExternalId', 'ExternalSystemId',  'IsPrimary', 'MarketCode', 'MarketId', 'OpportunityId', 'OpportunityName', 'Origin', 'OwnerId', 'PriceBookId', 'QuoteId', 'QuoteNumber', 'RevisionNumber', 'StatusId', 'StatusName']

    df = header_df(df, list_of_header_cols)

    return df