import src.aws_connection as aws
import os
from datetime import date, timedelta, datetime
import pandas as pd
import math
import pyarrow as pa
import pyarrow.parquet as pq
import pytz
from dotenv import load_dotenv

def lambda_handler(event, context):
    dataname = 'resignation'
    
    # read file configuration for calculation
    configuration = aws.read_json(os.environ['codebase_bucket'],
                                    os.environ['key_configuration_calculation'])
    
    # check if equipmentactivityservice calculation is active
    # if str(configuration[dataname]).lower() == 'yes':
    #     # check in configuration calculation if month_year parameter active or not
    #     if 'month_year' in event and event['month_year'] is not None:
    #         month_year = event['month_year']
    #     elif str(configuration['is_active_parameter_month_year(yes/no)']).lower()=='yes':
    #         month_year = configuration['month_year(mm_yyyy)']           
    #     else :
    #         month_year = (datetime.now(tz=pytz.timezone('Asia/Jakarta')).replace(day=1) - timedelta(days=1)).strftime("%m_%Y")
        # month_year = '06_2022'
        
    # define raw bucket
    configuration_bucket   = aws.read_json(os.environ['codebase_bucket'],os.environ['key_configuration_bucket'])
    bucket_raw             = '{}-{}'.format(os.environ['env'],configuration_bucket['raw'])
    bucket_landing         = '{}-{}'.format(os.environ['env'],configuration_bucket['landing'])

    # combine dataname with month_year for search latest file in s3
    prefix      = '{}/{}_{}'.format(dataname, dataname, '2023-10')
    latest_file = aws.get_latest_file(bucket_raw, prefix)
    print('{} latest file is {}'.format(dataname, latest_file))

    # read latest file for dataname and month_year
    data = aws.read_csv(bucket_raw, latest_file, dtype=str)
    print(data)


    #convert to integer for below column
    int_columns = ['rsg_ans_appid','rsg_annual_leave', 'rsg_annual_leave_before', 'rsg_excid']
    for col in int_columns:
        try :
            data[col] = data[col].apply(lambda x: int(x) if x not in ['', 'None'] and x.isdigit() else 0)
        except:
            pass

    # Convert specific columns to timestamp
    timestamp_columns = ['rsg_date', 'rsg_lastupdate', 'rsg_createddate']
    for col in timestamp_columns:
        data[col] = pd.to_datetime(data[col], format='%Y-%m-%d %H:%M:%S.%f')

    # Convert specific columns to datetime
    datetime_columns = ['rsg_effective_date', 'rsg_effective_date_before']
    for col in datetime_columns:
        data[col] = pd.to_datetime(data[col], format='%Y-%m-%d')
                
    # lower all column name
    data.columns = data.columns.str.lower()

    # store to landing bucket
    aws.to_parquet(data, bucket_landing, dataname,prefix=dataname)
    
    # define athena database
    #athena_database = ('{}_landing'.format(configuration_bucket[dataname])).replace('-','_')

load_dotenv()
lambda_handler('','')

