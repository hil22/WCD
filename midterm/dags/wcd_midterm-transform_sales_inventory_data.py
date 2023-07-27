# Note: pip install dotenv and requests beforehand


from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import avg, sum, mean, min, max, count, col, to_date, from_unixtime, date_format, when, desc, date_trunc
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType, LongType
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, weekofyear, dayofweek, year
from pyspark.sql import functions as F
import os
import sys
from datetime import datetime
import fnmatch

# ideally move these variables to a config file
RESULT_FOLDER = "result"  # folder that will be used for current output data 
CALENDAR_FILE_PREFIX = "calendar_"
INVENTORY_FILE_PREFIX = "inventory_"
PRODUCT_FILE_PREFIX = "product_"
SALES_FILE_PREFIX = "sales_"
STORE_FILE_PREFIX = "store_"
INPUT_FILES_SUFFIX = ".csv"
PARQUET_OUT_FOLDER_PREFIX = "weekly_summary"

### main ###
if __name__ == "__main__":
    # Retrieve the input arguments
    input_date = None
    s3_bucket = None
    input_prefix = None
    output_prefix = None   # this folder is used for archived output files

    # read in the system arguments to get all the path and filename info
    if len(sys.argv) > 1:
        input_date = sys.argv[sys.argv.index("--input_date") + 1] if "--input_date" in sys.argv else None
        s3_bucket = sys.argv[sys.argv.index("--bucket_name") + 1] if "--bucket_name" in sys.argv else None
        input_prefix = sys.argv[sys.argv.index("--input_bucket_prefix") + 1] if "--input_bucket_prefix" in sys.argv else None
        output_prefix = sys.argv[sys.argv.index("--output_bucket_prefix") + 1] if "--output_bucket_prefix" in sys.argv else None

    print("input_date : ", input_date)
    print("s3_bucket : ", s3_bucket)
    print("input_bucket_prefix : ",input_prefix)
    print("output_bucket_prefix : ",output_prefix)

    if not input_date or not s3_bucket or not input_prefix or not output_prefix:
        raise ValueError("Missing input arguments")

    # create spark session
    # test locally and then push to yarn EMR cluster after checking it works
    spark = SparkSession.builder.master("local[*]").appName("wcd_midterm_hn").getOrCreate()  

    ### Load the tables ###

    # load the files into dataframe tables (can read directly from S3 as airflow connector proves permissions)
    calendar_df = spark.read.option('header', 'true').option('delimiter', ',').csv("s3a://"+s3_bucket+"/"+input_prefix+"/"+CALENDAR_FILE_PREFIX+input_date+INPUT_FILES_SUFFIX)
    inventory_df = spark.read.option('header', 'true').option('delimiter', ',').csv("s3a://"+s3_bucket+"/"+input_prefix+"/"+INVENTORY_FILE_PREFIX+input_date+INPUT_FILES_SUFFIX)
    product_df = spark.read.option('header', 'true').option('delimiter', ',').csv("s3a://"+s3_bucket+"/"+input_prefix+"/"+PRODUCT_FILE_PREFIX+input_date+INPUT_FILES_SUFFIX)
    sales_df = spark.read.option('header', 'true').option('delimiter', ',').csv("s3a://"+s3_bucket+"/"+input_prefix+"/"+SALES_FILE_PREFIX+input_date+INPUT_FILES_SUFFIX)
    store_df = spark.read.option('header', 'true').option('delimiter', ',').csv("s3a://"+s3_bucket+"/"+input_prefix+"/"+STORE_FILE_PREFIX+input_date+INPUT_FILES_SUFFIX)
    print("Table files loaded")


    ### transform data ###

    # join fact tables on date so make the date column name the same in sales and inventory tables
    inventory_df = inventory_df.withColumnRenamed("cal_dt","trans_dt")

    # create a daily fact table which joins sales and inventory table (based on grain of prod_key and store_key and date) 
    daily_fact_df = inventory_df.join(sales_df, ['prod_key','store_key','trans_dt'])

    # add low_stock_flg to inventory table (flag is true when sales_qty is lower than stock_on_hand_qty)
    daily_fact_df = daily_fact_df.withColumn("low_stock_flg", when(col("sales_qty") < col("inventory_on_hand_qty"), 1).otherwise(0))

    # add day_of_week to be use for later to calc end of week inventory stats
    daily_fact_df = daily_fact_df.withColumn("day_of_week", dayofweek("trans_dt"))

    # add a column with a timestamp to identify the week/year (i.e. truncates dates to the first day of that week)
    daily_fact_df = daily_fact_df.withColumn("wk_timestamp", date_trunc("week", "trans_dt"))

    #daily_fact_df.show(10)

    # Add a flag for inventory statistics calc to know which entries represent EOW inventory (since inventory only recorded if a sale happens that day)
    # Lastest sales/inventory entry for that week becomes the EOW inventory day of week
    # Flag which inventory line item is the last for each week by year/week number/store key/product key
    window_spec = Window.partitionBy("wk_timestamp", "store_key", "prod_key")

    # Add the "last_inv_for_week_flag" column
    daily_fact_df = daily_fact_df.withColumn(
        "last_inv_for_week_flag",
        F.when(
            F.col("day_of_week") == F.max("day_of_week").over(window_spec),
            F.lit(True)
        ).otherwise(F.lit(False))
    )

    # create a weekly inventory table (by week, store, product) with business metrics requested
    weekly_inv_df = daily_fact_df.groupBy("wk_timestamp","store_key","prod_key")\
                    .agg(
                        (sum("sales_qty").alias("total_sales_qty")),\
                        (sum("sales_amt").alias("total_sales_amount")),\
                        ((sum("sales_amt")/sum("sales_qty")).alias("avg_sales_price")),\
                        (sum("sales_cost").alias("total_sales_cost")),\
                        (sum(when(col("last_inv_for_week_flag") == True, col("inventory_on_hand_qty")).otherwise(0))).alias("stock_level_EOW"),  
                        (sum(when(col("last_inv_for_week_flag") == True, col("inventory_on_order_qty")).otherwise(0))).alias("order_level_EOW"),       
                        (sum("out_of_stock_flg")/7).alias("percent_in_stock"),\
                        (sum("out_of_stock_flg")+sum("low_stock_flg")).alias("total_low_stock_impact"), \
                        (sum(when(col("low_stock_flg") == 1, col("sales_amt")-col("inventory_on_hand_qty")).otherwise(0))).alias("potential_low_stock_impact"),
                        (sum(when(col("out_of_stock_flg") == 1, col("sales_amt")).otherwise(0))).alias("no_stock_impact"),  
                        (sum("low_stock_flg")).alias("low_stock_instances"), \
                        (sum("out_of_stock_flg")).alias("no_stock_instances")
                    )

    # add the weeks the on hand stock can supply (do seperately as uses one of the agg columns)
    weekly_inv_df = weekly_inv_df.withColumn("weeks_on_hand_stock_can_supply", (weekly_inv_df.stock_level_EOW / weekly_inv_df.total_sales_qty))

    #weekly_inv_df.show(10)

    # save summary parquet file to S3 output folder for archiving
    # Note: partitioned to 4 files so faster for dev testing. In production change this to partitionBy() column based on usage of output
    weekly_inv_df.repartition(4).write.mode("overwrite").option("compression","gzip").parquet(f"s3a://"+s3_bucket+"/"+output_prefix+f"/{PARQUET_OUT_FOLDER_PREFIX}={input_date}")
    print("Output summary written to: "+"s3a://"+s3_bucket+"/"+output_prefix+f"/{PARQUET_OUT_FOLDER_PREFIX}={input_date}")

    # save summary parquet file to S3 result folder as the current data for BI consumption (no date in folder name so consistent folder for AWS Glue)
    weekly_inv_df.repartition(4).write.mode("overwrite").option("compression","gzip").parquet(f"s3a://"+s3_bucket+"/"+RESULT_FOLDER+f"/{PARQUET_OUT_FOLDER_PREFIX}")
    print("Output summary written to: "+f"s3a://"+s3_bucket+"/"+"result"+f"/{PARQUET_OUT_FOLDER_PREFIX}")


