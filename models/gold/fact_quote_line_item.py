from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *

def model(dbt, session):

    dbt.config(materialized="table")
    dbt.config(schema="cpq")

    cpq_quote_line_item_df = session.table("default_cpq.cpq_quote_line_item")
    new_column_name = ['currency_code', 'date_created', 'date_modified', 'distribution_channel', 'division', 'effective_date', 'error_message', 'external_id', 'external_system_id', 'is_primary', 'market_code', 'market_id', 'opportunity_id', 'opportunity_name', 'origin', 'owner_id', 'price_book_id', 'quote_id', 'quote_number', 'revision_number', 'status_id', 'status_name']
    final_df = cpq_quote_line_item_df.toDF(* new_column_name)

    load_date = dbt.config.get("load_date")
    final_df = final_df.withColumn("at_load_date", lit(load_date))

    return final_df