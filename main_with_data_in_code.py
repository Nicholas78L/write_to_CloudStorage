import os
import json
from google.cloud import bigquery
from google.cloud import storage
# pip install --upgrade google-cloud-storage

SERVICE_ACCOUNT_JSON = '/Users/a1/Desktop/keyForWritingInGoogleCloudeBQ.json'   # path to key

def write_in_local_file(file_name, data):
    print(f'Data is: {data}')
    with open(file_name, "w") as write_file:
        json.dump(data, write_file)

# (как будет называться файл и путь к нему в бакете, путь исходного файла который хотим сохранить, бакет нэйм)
def upload_to_bucket(blob_name, path_to_file, bucket_name):
    """ Upload data to a bucket"""
    # Explicitly use service account credentials by specifying the private key file.
    storage_client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_JSON)
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
    # table_id = 'your_project_id.your_dataset-id.your_table_name'.
    table_id = "writejsontocloudstorage.write_to_cs_bq.wr_to_cs_bq_table"
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("c_compiler_version", "STRING"),
            bigquery.SchemaField("channel_targets", "STRING"),
            bigquery.SchemaField("c_compiler", "STRING"),
            bigquery.SchemaField("CONDA_BUILD_SYSROOT", "STRING"),
            bigquery.SchemaField("target_platform", "STRING"),
        ],
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    uri = 'gs://wr_short_bucket/write_to_gc_bq/result_file_with_data_in_code.json'
    load_job = client.load_table_from_uri(
        uri,
        table_id,
        location="us-west2",  # Must match the destination dataset location.
        job_config=job_config,
    )  # Make an API request.
    load_job.result()  # Waits for the job to complete.
    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))

            # # # Все функции вызывать последовательно, по одной (остальные закоменчивать).
if __name__ == '__main__':
    data = {
      "c_compiler_version": "10",
      "channel_targets": "defaults",
      "c_compiler": "clang",
      "CONDA_BUILD_SYSROOT": "/opt/MacOSX10.10.sdk",
      "target_platform": "osx-64"
    }
            # # #  (куда сохранить данные и как назвать файл, имя переменной с данными)
    # write_in_local_file('source_json_files/local_file_with_data_in_code.json', data)
            # # #          (куда сохранить данные и как назвать файл,        откуда исходник,          имя_баккета)
    # upload_to_bucket('write_to_gc_bq/result_file_with_data_in_code.json', 'source_json_files/local_file_with_data_in_code.json', 'wr_short_bucket')
    gcs_to_bq()

