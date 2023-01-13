from pyspark.sql.functions import lit

def fun_explode_list(df, list_of_columns):
    for i in list_of_columns:
        df = df.select(f"{i}.*","*")
        df = df.drop(i)
    return df

def fun_add_column(df, column_name):
    df = df.withColumn("load_date", lit(column_name))
    return df


def model(dbt, session):

    dbt.config(materialized="table")

    df = session.table("default.raw")

    list_of_columns = ["AverageGrossMarginPercent", "AverageProductDiscountPercent", "Cost", "TotalAmount", "TotalListPrice", "TotalNetPrice", "TotalProductDiscountAmount"]
    df = fun_explode_list(df, list_of_columns)
    schedule_date = '2022-01-13'
    df = df.select("CurrencyCode", "CustomFields", "DateCreated", "DateModified", "DistributionChannel", "EffectiveDate", "ExternalId", "ExternalSystemId", "InvolvedParties", "IsPrimary")
    df = fun_add_column(df, schedule_date)
    df = df.filter(df.DistributionChannel != "")
    df1 = df.distinct()
    return df