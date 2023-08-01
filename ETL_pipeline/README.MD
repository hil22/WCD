README

ARCHITECTURE AND INSTRUCTIONS

For an explanation of the project and architecture, and instructions on how implement, refer to web blog at https://hazelby.co/?page_id=5. This page is also saved as the file "Orchestrate a batch ETL Data Pipeline with Airflow.pdf".

GITHUB REPO

Github files are located at https://github.com/hil22/WCD/tree/main/midterm

ASSUMPTIONS

- The project assumes you have set up snowflake or another DB to send the CSV input files to an S3 bucket.
- The project assumes you have an AWS account and knowledge of the AWS console and how to set IAM role permissions.
- This project will cost money to run since we will be using AWS services (EC2, EMR, Athena/Glue, S3, Lambda) will be used. Running EMR will be the most significant cost as it is creates multiple EC2 instances utilizing more compute.


