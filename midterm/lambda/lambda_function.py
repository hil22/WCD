
import boto3
import time
import json
import subprocess
from send_email import send_email
from datetime import datetime, timedelta

# move all these to lambda config variables
BUCKET_NAME = 'wcd-midterm-hn'
INPUT_PREFIX = 'input_from_trans_db'
OUTPUT_PREFIX = 'output'  # archiving folder
RESULT_PREFIX = 'result'  # latest files folder
RESULT_SUMMARY_PREFIX = 'weekly_summary'  # folder for the transformation output summary

# the input filenames (correspond to each input table)
CALENDAR_FILE_PREFIX = 'calendar_'
INVENTORY_FILE_PREFIX = 'inventory_'
SALES_FILE_PREFIX = 'sales_'
PRODUCT_FILE_PREFIX = 'product_'
STORE_FILE_PREFIX = 'store_'

AIRFLOW_EC2 = 'i-01ec7237f174ff6b5'
AIRFLOW_EC2_REGION = 'us-east-2'

# email notification info
SENDER = "hil222@gmail.com"
RECIPIENT = "hil222@gmail.com"

DAG_NAME = 'midterm_dag'

# making this easily adjustable if file lists requirements change
required_file_prefix_list = [CALENDAR_FILE_PREFIX, INVENTORY_FILE_PREFIX, SALES_FILE_PREFIX, STORE_FILE_PREFIX, PRODUCT_FILE_PREFIX]
copy_to_output_folder_prefix_list = [CALENDAR_FILE_PREFIX, STORE_FILE_PREFIX, PRODUCT_FILE_PREFIX]

# no event or context is being passed in as this is time schedule triggered
def lambda_handler(event, context):
    
    s3_file_list = []
    
    s3_client=boto3.client('s3')
    for object in s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=INPUT_PREFIX)['Contents']:
        s3_file_list.append(object['Key'])
    print("S3 file list :",s3_file_list)
    
    # use below to adjust for your timezone if needed since default is UTC (pretends to be the day before - needed after 5pm PST)
    datestr = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    required_file_list = [f'{INPUT_PREFIX}/{file_prefix}{datestr}.csv' for file_prefix in required_file_prefix_list]
    print("Required file list: ",required_file_list)
    
    # scan S3 bucket
    if set(required_file_list).issubset(set(s3_file_list)):
        s3_file_url = [f's3://{BUCKET_NAME}/{a}' for a in required_file_list]
        print('S3 file url : ',s3_file_url)
        table_name = [a.split('/')[1].split('_')[0] for a in required_file_list]
        print("table_name:", table_name)
    
        # change to pass the file date. More adjustable if the # of files change, then further in workflow read all the files with today's date in bucket
        #data = json.dumps({'conf':dict(zip(table_name, s3_file_url))})
        data = json.dumps({'conf': {'input_files_date': datestr}})
        print("data : ", data)
        
        # start the EC2 instance where airflow is located
        my_ec2_instance = AIRFLOW_EC2
        ec2_client = boto3.client('ec2', region_name=AIRFLOW_EC2_REGION)
        ec2_client.start_instances(InstanceIds=[my_ec2_instance])
        print('Starting EC2 instance: ' + my_ec2_instance)
        time.sleep(10) # give time for EC2 to start
     
        # get the public IP addr for the EC2 instance
        reservations = ec2_client.describe_instances(InstanceIds=[my_ec2_instance]).get("Reservations")
        instance_ip = reservations[0]['Instances'][0].get("PublicIpAddress")
        print("EC2 instance "+ my_ec2_instance+" has public IP "+ instance_ip)
     
        # copy the calendar, product, and store files to the output (per biz requirements)
        [s3_client.copy_object(Bucket=BUCKET_NAME, CopySource={'Bucket': BUCKET_NAME, 'Key': f'{INPUT_PREFIX}/{fi}{datestr}.csv'}, Key=f'{OUTPUT_PREFIX}/{fi}{datestr}.csv') for fi in copy_to_output_folder_prefix_list]
        print("Files copied to output bucket")

        # copy the calendar, product, and store files to the result to be used by glue to BI (result contains current files)
        [s3_client.copy_object(Bucket=BUCKET_NAME, CopySource={'Bucket': BUCKET_NAME, 'Key': f'{INPUT_PREFIX}/{fi}{datestr}.csv'}, Key=f'{RESULT_PREFIX}/{fi[:-1]}.csv') for fi in copy_to_output_folder_prefix_list]
        print("Files copied to result bucket")
     
        # send signal to Airflow    
        endpoint= "http:/" + instance_ip +":8080/api/v1/dags/"+DAG_NAME+"/dagRuns"
        print("Endpoint is ", endpoint)
    
        subprocess.run([
             'curl', 
             '-X', 
             'POST', 
             endpoint, 
             '-H',
             'Content-Type: application/json',
             '--user',
             'airflow:airflow',
             '--data', 
             data])
        print('File sent to Airflow : ', endpoint)
    else:
        print("Required file list not complete in S3 bucket")
        send_email(SENDER, RECIPIENT, AIRFLOW_EC2_REGION)
    
    
