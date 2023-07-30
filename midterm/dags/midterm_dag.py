import airflow
from airflow import DAG
from datetime import timedelta, datetime
import json
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr import EmrTerminateJobFlowOperator
from airflow.models.baseoperator import chain

#CLUSTER_ID = "j-37VIMVICWVO8T"     # use this temporarily for debugging

# variables to move to airflow xcom
TRANSFORM_CODE_FILE = 's3://wcd-midterm-hn/artifact/wcd_midterm-transform_sales_inventory_data.py'
BUCKET_NAME = 'wcd-midterm-hn'
INPUT_BUCKET_PREFIX= 'input_from_trans_db'
EMR_LOG_DIR = "s3://wcd-midterm-hn/emr-logs/"

# create spark step to run EMR
SPARK_STEPS = [
    {
        'Name': 'wcd_data_engineer',
        'ActionOnFailure': "CONTINUE",
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                '/usr/bin/spark-submit',
                #'--class', 'Driver.MainApp',
                '--master', 'yarn',
                '--deploy-mode', 'client',  # test in both client (for detailed logs) and cluster
                #'--num-executors', '2',
                # '--driver-memory', '512m',
                # '--executor-memory', '3g',
                # '--executor-cores', '2',
                TRANSFORM_CODE_FILE,
                #"{{ airflow.macros.ds_format(ds, '%Y-%m-$d', '&Y-%m-%d') }}",
                #'--spark_name', 'mid-term',
                '--bucket_name', BUCKET_NAME,
                '--input_bucket_prefix', INPUT_BUCKET_PREFIX,
                #'--data', "{{task_instance.xcom_pull('parse_request', key='input_paths')}}",
                '--input_date', "{{task_instance.xcom_pull('parse_request', key='input_files_date')}}",
                '--output_bucket_prefix', 'output',
                # '-c', 'job',
                # '-m', 'append',
                # '--input-options', 'header=true'
            ]
        }
    }

]

# parameters for EMR cluster creation
JOB_FLOW_OVERRIDES = {
    "Name": "wcd_midterm_cluster",
    "LogUri": EMR_LOG_DIR,
    "ReleaseLabel": "emr-6.10.0",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}], # We want our EMR cluster to have HDFS and Spark
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"}, 
                }
            ],
        }
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core - 2",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False, # this lets us programmatically terminate the cluster
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole"
}

# default args for dag
DEFAULT_ARGS = {
    'owner': 'wcd_data_engineer',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['airflow_data_eng@wcd.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

def retrieve_s3_files(**kwargs):
    # Access the 'data' key from kwargs
    data = kwargs['dag_run'].conf['input_files_date']
    kwargs['ti'].xcom_push(key = 'input_files_date', value = data)

dag = DAG(
    'midterm_dag',
    default_args = DEFAULT_ARGS,
    dagrun_timeout = timedelta(hours=2),
    schedule_interval = None,
    render_template_as_native_obj=True,
    start_date=datetime (2023,1,1)
)

parse_request = PythonOperator(task_id = 'parse_request',
                                provide_context = True, # Airflow will pass a set of keyword arguments that can be used in your function
                                python_callable = retrieve_s3_files,
                                dag = dag
                                ) 

start_task = DummyOperator(
    task_id = 'start_task',
    dag=dag
)

trigger_ge_dag = TriggerDagRunOperator(
    task_id = "trigger_great_expectations_dag",
    trigger_dag_id = "great_expectations.snowflake"
    #wait_for_completion=True,
    #deferrable=True,  # Note that this parameter only exists in Airflow 2.6+
)

# Create an EMR cluster
create_emr_cluster = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id="aws_default",
    emr_conn_id="emr_default",
    dag=dag
)

step_adder = EmrAddStepsOperator(
    task_id = 'add_steps',
    #job_flow_id = CLUSTER_ID,    # use in debugging mode
    job_flow_id = "{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}", # replace fixed CLUSTER_ID will pulling from xcom
    aws_conn_id = "aws_default", 
    steps = SPARK_STEPS,
    dag = dag
)

step_checker = EmrStepSensor(
    task_id = 'watch_step',
    #job_flow_id = CLUSTER_ID,   # use in debugging mode
    job_flow_id = "{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}", # replace fixed CLUSTER_ID will pulling from xcom
    step_id = "{{ task_instance.xcom_pull('add_steps', key='return_value')[0] }}",
    aws_conn_id = "aws_default",   
    dag = dag
)

glue_crawler_step = GlueCrawlerOperator(
    task_id="glue_crawler_step",
    config={"Name": "midterm_sales_summary_crawler"},
    dag=dag
)

# Terminate the EMR cluster
terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id="terminate_emr_cluster",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    dag=dag
)

end_task = DummyOperator(
    task_id='end_task',
    dag=dag
)

# main linear path of dag operators
chain(
    start_task,
    create_emr_cluster,
    parse_request,
    step_adder,
    step_checker,
    glue_crawler_step,
    terminate_emr_cluster,
    end_task
)

# run the Great Expectations suite checks seperately in parallel
start_task >> trigger_ge_dag