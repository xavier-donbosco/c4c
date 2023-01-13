def model(dbt, session):

    dbt.config(materialized="table")

    my_sql_model_df = session.table("default.raw")

    return my_sql_model_df