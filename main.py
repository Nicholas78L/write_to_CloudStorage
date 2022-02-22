import os
import json
from io import StringIO
from google.cloud import bigquery
from google.cloud import storage
# pip install --upgrade google-cloud-storage

SERVICE_ACCOUNT_JSON = '/Users/a1/Desktop/gsbq-key.json'   # path to key

def write_from_source_local_file_to_destination_local_file(source_file, destination_file):
    with open(source_file, "r") as read_file:
        data = json.load(read_file)
    result = [json.dumps(record) for record in data]
    with open(destination_file, 'w') as obj:
        for i in result:
            obj.write(i + '\n')

# # # (как будет называться файл и путь к нему в бакете, путь исходного файла который хотим сохранить, бакет нэйм)
def upload_to_bucket(blob_name, path_to_file, bucket_name):
    """ Upload data to a bucket"""
    # # # Explicitly use service account credentials by specifying the private key file.
    storage_client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_JSON)
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(path_to_file)
    print('blob.public_url', blob.public_url)
    return blob.public_url

def gcs_to_bq():
    table_id = ''
    # # # Construct a BigQuery client object.
    client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_JSON)
    # TODO(developer): Set table_id to the ID of the table to create.
    # # # table_id = 'your_project_id.your_dataset-id.your_table_name'.
    table_id = "gsbq-341910.cs_dataset_id.cs_bg_table"
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("name", "STRING"),     # # # Поля Схемы должны соответствовать полям таблицы dataset в Google Cloud Storage
            bigquery.SchemaField("price", "STRING"),
            bigquery.SchemaField("sum1", "STRING"),
            bigquery.SchemaField("quantity", "STRING"),
        ],
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    uri = 'gs://wrjsontogcsbq/write_to_gc_bq/result_file.json'
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
            # # # write_from...() эта ф-ция позволяет переформатировать JSON-й файл в ND-JSON файл (newline delimited JSON file) для распознания его таблицами BigQuery.
    write_from_source_local_file_to_destination_local_file("aa_engl.json", 'nd-proceesed.json')  ## (откуда:файл-исходник, куда:будущий-файл-адресат)
            # # #  os.system( copy /путь к исходнику данных    /куда положить в PyCharm и как назвать файл ) ф-ция копирует файл целиком.
    # os.system('cp /Users/a1/PycharmProjects/write_to_CloudStorage/nd-proceesed.json /Users/a1/PycharmProjects/write_to_CloudStorage/temp/local_file.json')
            # # #  upload_to_bucket(в какой dataset сохранить данные и как назвать файл (таблицу), откуда исходник, имя_баккета) ф-ция закачивает файл в bucket.
    # upload_to_bucket('write_to_gc_bq/result_file.json', 'temp/local_file.json', 'wrjsontogcsbq')
    # gcs_to_bq()


