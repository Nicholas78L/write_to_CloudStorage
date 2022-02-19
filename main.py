import json
from google.cloud import bigquery
from google.cloud import storage
# pip install --upgrade google-cloud-storage

SERVICE_ACCOUNT_JSON = r'/Users/a1/Desktop/gcp_test_key.json'

def write_in_local_file(file_name, data):
    print(f'Data is: {data}')
    with open(file_name, "w") as write_file:
        json.dump(data, write_file)

def upload_to_bucket(blob_name, path_to_file, bucket_name): #(как будет называться файл и путь к нему в бакете, путь исходного файла который хотим сохранить, бакет нэйм)
    """ Upload data to a bucket"""

    # Explicitly use service account credentials by specifying the private key
    # file.
    storage_client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_JSON)

    #print(buckets = list(storage_client.list_buckets())

    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(path_to_file)
    print('blob.public_url', blob.public_url)
    return blob.public_url

def gcs_to_bq():
    table_id = ''
    # Construct a BigQuery client object.
    client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_JSON)

    # TODO(developer): Set table_id to the ID of the table to create.
    table_id = "scrappers-341616.gcp_test.example1"

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("post_abbr", "STRING"),
        ],
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    uri = 'gs://gcp_example_bucket/gcp_testing/cloud_file.json'

    load_job = client.load_table_from_uri(
        uri,
        table_id,
        location="US",  # Must match the destination dataset location.
        job_config=job_config,
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))

if __name__ == '__main__':
    data = {
        "name": "Zaphod Beeblebrox",
        "post_abbr": "Betelgeusian Street"
    }

    write_in_local_file('tmp/local_file.json', data)
    upload_to_bucket('gcp_testing/cloud_file.json', 'tmp/local_file.json', 'gcp_example_bucket')
    gcs_to_bq()

