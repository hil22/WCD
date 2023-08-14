# This code creates a dynamoDB table if it doesn't already exist. 
# Then it reads a specific "year" stated in a config.json file and pulls the data from that year and stores in the DynamoDB table.

import json
import boto3
import csv
import io
from datetime import datetime

config_file = 'config/config.json'
table_name = "dynamoClimateTable"
region_name = "us-east-2"

def create_dyn_table(dyn_resource=None):
    # Creates a DynamoDB table
    # param dyn_resource: either a Boto3 or DAX resrouce
    # returns: the newly created table
    
    try: 
        # check if table exists by running list tables
        client = boto3.client('dynamodb')
        response = client.list_tables()
        
        # if the table exists then exit and return the table
        if table_name in response['TableNames']:
            print('table exists')
            return None
        # if the table doesn't exist then create it here
        else:
        
            # create the boto client resource if needed
            if dyn_resource is None:
                dyn_resource = boto3.resource('dynamodb', region_name=region_name)
                
            params = {
                'TableName': table_name,
                "KeySchema": [
                    {'AttributeName': 'year', 'KeyType': 'HASH'},
                    {'AttributeName': 'date', 'KeyType': 'RANGE'}
                ],
                'AttributeDefinitions': [
                    {'AttributeName': 'year', 'AttributeType': 'S'},
                    {'AttributeName': 'date', 'AttributeType': 'S'}
                ],
                'ProvisionedThroughput': {
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5
                }
            }
            
            table = dyn_resource.create_table(**params)
            print(f"Creating {table_name}...")
            table.wait_until_exists()
            return table
            
    except Exception as e:
        print('Exception caught. DynamoDB not created. ', str(e))    

def readYear(bucket, key):
    # reads opens the key file in the bucket location and reads the year from it
    
    try:
        s3 = boto3.client('s3')
    
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response['Body']
        config = json.loads(content.read())
        year = config['year']
        print("The input data year is ", year)
        return year
        
    except Exception as e:
        print('Exception caught.Year not read. ', str(e))        

def writeToDb(bucket, key, dynamoTableName, year):
    #start reading the rows from the csv iput file
    
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(dynamoTableName)
    
    try:    
        s3_client = boto3.client('s3')
        s3_response_object = s3_client.get_object(Bucket=bucket, Key=key)
        object_content = s3_response_object['Body'].read().decode('utf-8')

        csv_reader = csv.DictReader(io.StringIO(object_content))

        # use a batch_writer to perform batch write operations
        with table.batch_writer() as batch:
            # traverse the input file rows
            for row in csv_reader:
                # if a row matching the wanted extraction year is found
                if row.get('Year') == str(year):  
                    
                    original_date = datetime.strptime(row['Date/Time'], '%m/%d/%y')
                    iso_date = original_date.strftime('%Y-%m-%d')
                    
                    # create an item to store in db
                    item = {
                        'year': row['Year'],
                        'date': iso_date
                    }

                    # iterate over the key value pairs (dictionary format) i.e. in one row of the csv file, each column
                    for key1, value in row.items():
                        # if this key isn't one of the primary key fields
                        if key1 not in ['Year', 'Date/Time'] and value.strip():
                            # add the field to the item (removing special chars)
                            item[key1.lower().replace(' ', '-').replace(')', '').replace('(','').replace('°c', 'c')] = value
	 
	                # insert the item into DynamoDB
                    batch.put_item(Item=item)
                    #print("Item added to dynamoDB table ", item)
        
    except Exception as e:
        print('Exception caught. Database write error. ', str(e))   

def lambda_handler(event, context):
    
    # get the bucket and file name from the incoming event
    input_bucket = event['Records'][0]['s3']['bucket']['name']
    input_file = event['Records'][0]['s3']['object']['key']    
    print("Input bucket: ", input_bucket)
    print("Input file: ", input_file)
    
    # create dynamoDb table if needed
    create_dyn_table(None)
    
    # read the year from config json
    year = readYear(input_bucket, config_file)
        
    writeToDb(input_bucket, input_file, table_name, year)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Lamba executed successfully')
    }
    