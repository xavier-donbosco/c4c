from pyspark.sql import functions as f
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
from pyspark.sql import SQLContext, Row
from datetime import datetime
from datetime import timedelta
from pyspark.sql import DataFrame
import json
import re
import ast
import requests
import json
from functools import reduce
from pyspark.sql.functions import col,lit

def addColumnsToDataFrame(df: DataFrame, list_of_col: list): 
    addColsDF = (reduce(lambda df_temp, x: df_temp.withColumn(x, lit(x)), list_of_col, df))
    concatDF = (reduce(lambda df_temp, x: df_temp.withColumn(x, concat(col("INVOLVEDPARTIES_PARTNERFUNCTIONNAME"), lit("_"), (col(x)))), list_of_col, addColsDF))
    return concatDF

def pivotSelectedCols(df: DataFrame, dic):
    l = []
    l.append((reduce(lambda x, v: df.groupBy("QuoteId").pivot(v[0]).agg(first(v[1])), dic.items())))
    return l

def model(dbt, session):

    dbt.config(materialized="table")
    dbt.config(schema="cpq")

    cpq_involved_parties_df = session.table("default_cpq.cpq_involved_parties").withColumnRenamed("PartnerFunctionName" , "INVOLVEDPARTIES_PARTNERFUNCTIONNAME")

    involvedParties_names = ["Ship-to party", "Regional Manager", "Distributor Contact", "Payer", "District  Manager", "Sales representative", "Project Location", "Distributor", "Sold-to party", "Contact person", "Bill-to party"]

    partnerNamesDF = session.createDataFrame(involvedParties_names, "string").toDF("INVOLVEDPARTIES_PARTNERFUNCTIONNAME")
    quotid_list = cpq_involved_parties_df.select("QuoteId").distinct()
    crossJoinQuoteItemDF = quotid_list.crossJoin(partnerNamesDF)

    involvedPartiesDF = crossJoinQuoteItemDF.join(cpq_involved_parties_df, ["QuoteId", "INVOLVEDPARTIES_PARTNERFUNCTIONNAME"], "left")

    list_of_common_col = ["NO", "NM", "FIRST_NM", "LAST_NM", "COUNTRY", "STATE", "CITY_NM", "POSTAL_CD", "ADDRESS_NM", "EMAIL_ADDRESS", "PHONE_NO", "STREET_NM"]

    demo1 = addColumnsToDataFrame(involvedPartiesDF, list_of_common_col)

    li = {"NO":"ExternalId", "ADDRESS_NM":"AddressName", "FIRST_NM":"FirstName", "LAST_NM":"LastName", "NM":"Name", "PHONE_NO":"Phone", "POSTAL_CD":"PostalCode", "EMAIL_ADDRESS":"EmailAddress", "STATE":"STATE", "COUNTRY":"COUNTRY", "CITY_NM":"CityName"}

    list_of_involved_dfs = pivotSelectedCols(demo1, li)

    cpq_involved_parties_df= reduce(lambda left, right: left.join(right, on=["QuoteId"]), list_of_involved_dfs)

    new_list= ['QuoteId', 'Bill_to_party_CITY_NM', 'Contact_person_CITY_NM', 'Distributor_Contact_CITY_NM', 'Distributor_CITY_NM', 'District_Manager_CITY_NM', 'Payer_CITY_NM', 'Project_Location_CITY_NM', 'Regional_Manager_CITY_NM', 'Sales_representative_CITY_NM', 'Ship_to_party_CITY_NM', 'Sold_to_party_CITY_NM']
    cpq_involved_parties_df = cpq_involved_parties_df.toDF(* new_list)

    cpq_custom_fields_df = session.table("default_cpq.cpq_custom_fields")
    cpq_header_df = session.table("default_cpq.cpq_header")
    cpq_quote_amount_df = session.table("default_cpq.cpq_quote_amount")

    final_joined_df = cpq_custom_fields_df.join(cpq_header_df, on = ["QuoteId"])\
                                      .join(cpq_involved_parties_df, on = ["QuoteId"])\
                                      .join(cpq_quote_amount_df, on = ["QuoteId"])

    return final_joined_df