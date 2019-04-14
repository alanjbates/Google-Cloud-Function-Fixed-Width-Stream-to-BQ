from google.cloud import bigquery
import pandas as pd
import numpy as np
import gcsfs

# [START function]
def streamtobq(data, context):

    print('Event ID: {}'.format(context.event_id))
    print('Event type: {}'.format(context.event_type))
    print('Bucket: {}'.format(data['bucket']))
    print('File: {}'.format(data['name']))
    print('Created: {}'.format(data['timeCreated']))

    #create gs:// path based on input variables
    gs_source_bucket = data['bucket']
    gs_source_file = data['name']
    gs_source_path = 'gs://'+gs_source_bucket+'/'+gs_source_file
    loadfile(gs_source_path)

def loadfile(gs_source_path):

    #open the file from GCS into memory.  Since these files are extremely small, this works inside the cloud function limits
    df = pd.read_csv(gs_source_path, header=None, names=['raw'])

    #if file is empty, exit function gracefully, else process file and load to bq
    if df.raw.count() == 0:
        print('file contains zero records, skip file')
    else:
        #split fixed with raw string into columns
        df['dimension_one'] = df.raw.str[:5]
        df['date_time'] = df.raw.str[5:19]
        df['device_id'] = df.raw.str[19:21]
        df['action'] = df.raw.str[21:22]

        #convert ugly string to date_time
        df['date_time'] = pd.to_datetime(df['date_time'], format='%Y%m%d%H%M%S')
        #convert back to string to avoid array timestamp to bq error :/
        df['date_time'] = df.date_time.astype(str)

        #add datadate column to frame with current timestamp for auditing
        df.insert(0, 'datadate', pd.datetime.now().replace(microsecond=0))
        #convert back to string to avoid array timestamp to bq error :/
        df['datadate'] = df.datadate.astype(str)

        #remove unwanted pandas index column so it doesn't show up in the array
        df.drop('raw', axis=1, inplace=True)

        #set BQ load params
        bqclient = bigquery.Client(project='your-gcp-project-name')
        dataset_ref = bqclient.dataset('your-bq-dataset')
        table_ref = dataset_ref.table('your-bq-table')
        table = bqclient.get_table(table_ref)

        #convert to array for BQ streaming insert
        data = df.to_numpy()

        #streaming insert to BQ
        bqclient.insert_rows(table, data)

# [END function]
