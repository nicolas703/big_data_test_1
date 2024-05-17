import os
import io
from google.cloud import bigquery
from google.cloud import storage

project_id = os.environ['PROJECT']
bq_dataset = os.environ['DATASET']
bq_table = os.environ['TABLE']
bucket_name = os.environ["BUCKET"]
file_name = os.environ['FILE']
bigquery_client = bigquery.Client()
dataset_ref = bigquery_client.dataset(bq_dataset)

storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)



def psyco_loader(event, context):
        """Triggered by a change to a Cloud Storage bucket.
    Args:
        event (dict): The event payload.
        context (google.cloud.functions.Context): Metadata for the event.
    """
        print("event_key {}".format(event))
        blob = bucket.blob(file_name + ".csv")
        file_content = blob.download_as_string().decode('utf-8')
        print("file_content: ", file_content)

        table_ref = dataset_ref.table(bq_table)
        
        job_config = bigquery.LoadJobConfig(
                schema =  [
                        bigquery.SchemaField('ID', 'INTEGER', mode='REQUIRED'),
                        bigquery.SchemaField('age', 'STRING'),
                        bigquery.SchemaField('gender', 'STRING'),
                        bigquery.SchemaField('occupation', 'STRING'),
                        bigquery.SchemaField('line_of_work', 'STRING'),
                        bigquery.SchemaField('time_bp', 'INTEGER'),
                        bigquery.SchemaField('time_dp', 'INTEGER'),
                        bigquery.SchemaField('travel_time', 'INTEGER'),
                        bigquery.SchemaField('easeof_online', 'INTEGER'),
                        bigquery.SchemaField('home_env', 'INTEGER'),
                        bigquery.SchemaField('prod_inc', 'INTEGER'),
                        bigquery.SchemaField('sleep_bal', 'INTEGER'),
                        bigquery.SchemaField('new_skill', 'INTEGER'),
                        bigquery.SchemaField('fam_connect', 'INTEGER'),
                        bigquery.SchemaField('relaxed', 'INTEGER'),
                        bigquery.SchemaField('self_time', 'INTEGER'),
                        bigquery.SchemaField('like_hw', 'INTEGER'),
                        bigquery.SchemaField('dislike_hw', 'INTEGER'),
                        bigquery.SchemaField('prefer', 'STRING'),
                        bigquery.SchemaField('certaindays_hw', 'STRING'),
                        bigquery.SchemaField('column1', 'INTEGER'),
                        bigquery.SchemaField('travel_work', 'INTEGER'),
                        bigquery.SchemaField('loaded', 'BOOLEAN', mode='REQUIRED')
                    ],
                skip_leading_rows=1, source_format = bigquery.SourceFormat.CSV,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                field_delimiter=','
        )
        
        file_obj = io.StringIO(file_content)

        load_job = bigquery_client.load_table_from_file(
                file_obj,
                table_ref,
                job_config=job_config)
        
        print('Starting job {}'.format(load_job.job_id))
        print('File: {}'.format(file_name))
        load_job.result()
        print('Job finished.')
        destination_table = bigquery_client.get_table(table_ref)
        print('Loaded {} rows.'.format(destination_table.num_rows))