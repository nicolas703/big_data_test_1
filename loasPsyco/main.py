import os
import io
import uuid
import csv
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

def generate_uuid():
    return str(uuid.uuid4())

def psyco_loader(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
        event (dict): The event payload.
        context (google.cloud.functions.Context): Metadata for the event.
    """
    print("Event details: {}".format(event))
    blob = bucket.blob(file_name + ".csv")
    file_content = blob.download_as_string().decode('utf-8')
    print("File content: ", file_content)

    table_ref = dataset_ref.table(bq_table)
    
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField('ID', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('age', 'STRING'),
            bigquery.SchemaField('gender', 'STRING'),
            bigquery.SchemaField('occupation', 'STRING'),
            bigquery.SchemaField('line_of_work', 'STRING'),
            bigquery.SchemaField('time_bp', 'NUMERIC'),
            bigquery.SchemaField('time_dp', 'NUMERIC'),
            bigquery.SchemaField('travel_time', 'NUMERIC'),
            bigquery.SchemaField('easeof_online', 'NUMERIC'),
            bigquery.SchemaField('home_env', 'INTEGER'),
            bigquery.SchemaField('prod_inc', 'NUMERIC'),
            bigquery.SchemaField('sleep_bal', 'NUMERIC'),
            bigquery.SchemaField('new_skill', 'NUMERIC'),
            bigquery.SchemaField('fam_connect', 'NUMERIC'),
            bigquery.SchemaField('relaxed', 'NUMERIC'),
            bigquery.SchemaField('self_time', 'NUMERIC'),
            bigquery.SchemaField('like_hw', 'NUMERIC'),
            bigquery.SchemaField('dislike_hw', 'NUMERIC'),
            bigquery.SchemaField('prefer', 'STRING'),
            bigquery.SchemaField('certaindays_hw', 'STRING'),
            bigquery.SchemaField('loaded', 'BOOLEAN', mode='REQUIRED')
        ],
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        field_delimiter=','
    )

    # Convert CSV content to a list of dictionaries and add UUID and loaded fields
    csv_reader = csv.DictReader(io.StringIO(file_content))
    original_fieldnames = csv_reader.fieldnames
    fieldnames = ['ID'] + original_fieldnames + ['loaded']  # Ensure ID at the beginning and loaded at the end

    data_with_uuid_and_loaded = []
    for row in csv_reader:
        new_row = {'ID': generate_uuid()}
        new_row.update(row)
        new_row['loaded'] = False
        data_with_uuid_and_loaded.append(new_row)

    # Convert the list of dictionaries back to a CSV string
    csv_output = io.StringIO()
    csv_writer = csv.DictWriter(csv_output, fieldnames=fieldnames)
    csv_writer.writeheader()
    csv_writer.writerows(data_with_uuid_and_loaded)
    csv_output.seek(0)  # Reset the StringIO object for reading

    # Print the final CSV content for debugging
    print("Final CSV content:\n", csv_output.getvalue())
    csv_output.seek(0)  # Reset to the beginning of the StringIO object

    load_job = bigquery_client.load_table_from_file(
        csv_output,
        table_ref,
        job_config=job_config
    )
    
    print('Starting job {}'.format(load_job.job_id))
    print('File: {}'.format(file_name))
    
    try:
        load_job.result()  # Wait for the load job to complete.
        print('Job finished.')
        destination_table = bigquery_client.get_table(table_ref)
        print('Loaded {} rows.'.format(destination_table.num_rows))
    except Exception as e:
        print(f'Error during load: {e}')
