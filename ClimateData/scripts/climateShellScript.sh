#!/bin/bash

# later change to use base folder in each export
# set variables
filenametime1=$(date +"+%Y%m%d_%H%M%S")
stationId=48549
month=2   # for February
webSite='https://climate.weather.gc.ca/climate_data/bulk_data_e.html'

export BASE_FOLDER='/home/hn/WCDAssignments/ClimateData/'
export SCRIPTS_FOLDER=${BASE_FOLDER}/'scripts'
export INPUT_FOLDER=${BASE_FOLDER}/'input'
export OUTPUT_FOLDER=${BASE_FOLDER}/'output'
export LOG_FOLDER=${BASE_FOLDER}/'logs'
export SHELL_SCRIPT_NAME='climateShellScript'
export LOG_FILE=${LOG_FOLDER}/${SHELL_SCRIPT_NAME}_${filenametime1}.log

#set logs
exec 1> >(tee ${LOG_FILE} 2>&1)
#exec 2> >(tee ${LOG_FILE} >&2)

# provided code to scrape from website
for year in {2020..2022};
# -N adds timestamping. What is --content-disposition?. -o is output file. Day is not used so can be anything.
do wget -N --content-disposition "${webSite}?format=csv&stationID=${stationId}&Year=${year}&Month=${month}&Day=14&timeframe=1&submit= Download+Data" -O ${INPUT_FOLDER}/${year}.csv
done;

# monitor the job result of wget
RC1=$?
if [ ${RC1} != 0 ];
then
    echo "The climate data download failed" >&2
else
    echo "The climate data downloaded successfully"
fi

# jobs
python3 ${SCRIPTS_FOLDER}/climateScript.py

# monitor the job result of wget
RC1=$?
if [ ${RC1} != 0 ];
then
    echo "The climate python script failed" >&2
    #mail -s "Climate data task failed" hil222@gmail.com <<< "the task failed"
    exit 1
else
    echo "The climate python script ran successfully"
    #mail -s "Climate data task successed" hil222@gmail.com <<< "the task succeeded"
    exit 0
fi
