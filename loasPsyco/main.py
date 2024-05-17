import os
import io
import uuid
from google.cloud import bigquery
from google.cloud import storage

project_id = os.environ['PROJECT']
bq_dataset = os.environ['DATASET']
bq_table = os.environ['TABLE']
bucket_name = os.environ["BUCKET"]
file_prefix = os.environ['FILE']  # Esto ahora es un prefijo, no el nombre completo del archivo
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
    # Generamos un UUID y construimos el nombre del archivo
    unique_file_name = f"{uuid.uuid4()}_{file_prefix}.csv"
    
    print("Processing file: {}".format(unique_file_name))
    blob = bucket.blob(unique_file_name)
    file_content = blob.download_as_string().decode('utf-8')

    table_ref = dataset_ref.table(bq_table)
    
    job_config = bigquery.LoadJobConfig(
        schema = [
            bigquery.SchemaField('ID', 'INTEGER', mode='REQUIRED'),
            bigquery.SchemaField('Age', 'STRING'),
            bigquery.SchemaField('Gender', 'STRING'),
            bigquery.SchemaField('Occupation', 'STRING'),
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
            bigquery.SchemaField('time_bp:1', 'INTEGER'),
            bigquery.SchemaField('travel_work', 'INTEGER'),
            bigquery.SchemaField('loaded', 'BOOLEAN')
        ],
        skip_leading_rows=1,
        source_format = bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        field_delimiter=','
    )
    
    file_obj = io.StringIO(file_content)
    load_job = bigquery_client.load_table_from_file(
        file_obj,
        table_ref,
        job_config=job_config
    )
    
    print('Starting job {}'.format(load_job.job_id))
    load_job.result()
    print('Job finished.')
    destination_table = bigquery_client.get_table(table_ref)
    print('Loaded {} rows.'.format(destination_table.num_rows))
