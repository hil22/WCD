
import boto3
import time
import json
import subprocess
from send_email import send_email
from datetime import datetime, timedelta

def lambda_handler(event, context):
    
    s3_file_list = []
    
    s3_client=boto3.client('s3')
    for object in s3_client.list_objects_v2(Bucket='wcd-midterm-hn', Prefix='input_from_trans_db')['Contents']:
        s3_file_list.append(object['Key'])
    print("S3 file list :",s3_file_list)
    
    # use below to adjust for your timezone if needed since default is UTC (pretends to be the day before - needed after 5pm PST)
    datestr = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    required_file_list = [f'input_from_trans_db/calendar_{datestr}.csv', f'input_from_trans_db/inventory_{datestr}.csv', f'input_from_trans_db/product_{datestr}.csv', f'input_from_trans_db/sales_{datestr}.csv', f'input_from_trans_db/store_{datestr}.csv']
    print("Required file list: ",required_file_list)
    
    # scan S3 bucket
    if set(required_file_list).issubset(set(s3_file_list)):
        

        # Creates a list of files for transformation. This could be passed to airflow but changed to pass the file date instead
        # s3_file_url = ['s3://' + 'wcd-midterm-hn/' + a for a in required_file_list]
        # print('S3 file url : ',s3_file_url)
        # table_name = [a.split('/')[1].split('_')[0] for a in required_file_list]
        # print("table_name:", table_name)
    
        # send the date of the current files to airflow
        data = json.dumps({'conf': {'input_files_date': datestr}})
        print("data : ", data)
        
        # start the EC2 instance where airflow is located
        my_ec2_instance = 'i-01ec7237f174ff6b5'
        ec2_client = boto3.client('ec2', region_name='us-east-2')
        ec2_client.start_instances(InstanceIds=[my_ec2_instance])
        print('Starting EC2 instance: ' + my_ec2_instance)
        time.sleep(10) # give time for EC2 to start
     
        # get the public IP addr for the EC2 instance
        reservations = ec2_client.describe_instances(InstanceIds=[my_ec2_instance]).get("Reservations")
        instance_ip = reservations[0]['Instances'][0].get("PublicIpAddress")
        print("EC2 instance "+ my_ec2_instance+" has public IP "+ instance_ip)
     
        # copy the calendar, product, and store files to the output (per biz requirements)
        s3_client.copy_object(Bucket='wcd-midterm-hn', CopySource={'Bucket': 'wcd-midterm-hn', 'Key': f'input_from_trans_db/calendar_{datestr}.csv'}, Key='output/calendar_{datestr}.csv')
        s3_client.copy_object(Bucket='wcd-midterm-hn', CopySource={'Bucket': 'wcd-midterm-hn', 'Key': f'input_from_trans_db/product_{datestr}.csv'}, Key='output/product_{datestr}.csv')
        s3_client.copy_object(Bucket='wcd-midterm-hn', CopySource={'Bucket': 'wcd-midterm-hn', 'Key': f'input_from_trans_db/store_{datestr}.csv'}, Key='output/store_{datestr}.csv')
        print("Files copied to output bucket")

        # copy the calendar, product, and store files to the result to be used by glue to BI (result contains current files)
        s3_client.copy_object(Bucket='wcd-midterm-hn', CopySource={'Bucket': 'wcd-midterm-hn', 'Key': f'input_from_trans_db/calendar_{datestr}.csv'}, Key='result/calendar.csv')
        s3_client.copy_object(Bucket='wcd-midterm-hn', CopySource={'Bucket': 'wcd-midterm-hn', 'Key': f'input_from_trans_db/product_{datestr}.csv'}, Key='result/product.csv')
        s3_client.copy_object(Bucket='wcd-midterm-hn', CopySource={'Bucket': 'wcd-midterm-hn', 'Key': f'input_from_trans_db/store_{datestr}.csv'}, Key='result/store.csv')
        print("Files copied to result bucket")
     
        # send signal to Airflow    
        endpoint= "http:/" + instance_ip +":8080/api/v1/dags/midterm_dag/dagRuns"
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
        send_email()
    
    
