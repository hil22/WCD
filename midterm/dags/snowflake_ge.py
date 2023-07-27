import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator

# Tables for Great Expectation check
inv_table = "raw.inventory"
sales_table = "raw.sales"
ge_root_dir = "/opt/airflow/great_expectations"
SNOWFLAKE_CONN_ID =  "snowflake_conn"

dag = DAG(
    "great_expectations.snowflake",
    start_date=datetime (2023,1,1),
    description="Example DAG showcasing loading and data quality checking with Snowflake and Great Expectations.",
    schedule_interval=None,
    doc_md=__doc__,
    template_searchpath="opt/airflow/sql",
    catchup=False,
)

# Keep commented out section to use another time when need to load and create tables in snowflake first (rather than now when we already have loaded)
"""
### Snowflake table creation
Create the table to store sample data
"""

# create_table = SnowflakeOperator(
#     task_id="create_table",
#     snowflake_conn_id=SNOWFLAKE_CONN_ID,
#     sql="{% include 'create_snowflake_yellow_tripdata_table.sql' %}",
#     params=("table_name":table),
#     dag=dag
# )

# ### Insert data
# ### Insert data intot the snowflake table using an existing SQL query (store in the ~/data/airflow/sql directory)
# load_table = SnowflakeOperator(
#     task_id="insert_query",
#     snowflake_conn_id=SNOWFLAKE_CONN_ID,
#     sql="{% include 'load_yellow_tripdata_table.sql' %}",
#     params=("table_name":table),
#     dag=dag
# )

# ### Delete table
# ### Clear up the table created for the example.

# delete_table = SnowflakeOperator (
#     task_id="delete_table",
#     snowflake_conn_id=SNOWFLAKE_CONN_ID,
#     sql="{% include 'delete_snowflake_table.sql' %}",
#     params=("table_name":table),
#     dag=dag
# )

start_task = DummyOperator(
    task_id = 'start_task',
    dag=dag
)

### Great Expectation Suite
## Run the Great Expectations suite on the snowflake tables

ge_snowflake_inv_validation=GreatExpectationsOperator(
    task_id="ge_snowflake_inv_validation",
    data_context_root_dir=ge_root_dir,
    conn_id=SNOWFLAKE_CONN_ID,
    expectation_suite_name="input_data_snowflake_check",
    data_asset_name=inv_table,
    fail_task_on_validation_failure=False,
    return_json_dict=True,
    dag=dag
)

ge_snowflake_sales_validation=GreatExpectationsOperator(
    task_id="ge_snowflake_sales_validation",
    data_context_root_dir=ge_root_dir,
    conn_id=SNOWFLAKE_CONN_ID,
    expectation_suite_name="sales_data_check",
    data_asset_name=sales_table,
    fail_task_on_validation_failure=False,
    return_json_dict=True,
    dag=dag
)

# Use this query just a subset of the info when the dataset becomes very large
# ge_snowflake_query_validation=GreatExpectationsOperator(
#     task_id="ge_snowflake_query_validation",
#     data_context_root_dir=ge_root_dir,
#     conn_id=SNOWFLAKE_CONN_ID,
#     query_to_validate="SELECT *",
#     expectation_suite_name="input_data_snowflake_check",
#     data_asset_name=table,
#     fail_task_on_validation_failure=False,
#     return_json_dict=True,
#     dag=dag
# )

end_task = DummyOperator(
    task_id='end_task',
    dag=dag
)

chain(
    start_task,
    [
        ge_snowflake_inv_validation,
        ge_snowflake_sales_validation
    ],
    end_task
)
