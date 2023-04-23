#!usr/bin python3

import pandas as pd
import glob

input_path = '/home/hn/WCDAssignments/ClimateData/input'
output_path = '/home/hn/WCDAssignments/ClimateData/output'
concatFilename = 'all_years.csv'

csvFiles = glob.glob(input_path+'/*.csv', recursive=False)

print ('Starting to combine files')

df=pd.DataFrame()

for file in csvFiles:
        data = pd.read_csv(file)
        df = pd.concat([df, data], axis=0)
        print ('Appending file: '+file)
df.to_csv(output_path+'/'+concatFilename, index=False)

# WCD solution
# csvs = glob.glob(os.path.join(input_path, "*.csv"))
#
# li = []
# for f in csvs;
#       df = pd.read_csv(f, index_col=None, header=0)
#       li = append(df)
#
# df_concat = pd.concat(li,axis=0, ignore_iindex=True)
#
#df_concat.to_csv(output_path+'/'+filname)
