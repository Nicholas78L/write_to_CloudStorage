import os
import json
from google.cloud import bigquery
from google.cloud import storage
# pip install --upgrade google-cloud-storage

SERVICE_ACCOUNT_JSON = '/Users/a1/Desktop/keyForWritingInGoogleCloudeBQ.json'   # path to key

    # # # (как будет называться файл и путь к нему в бакете, путь исходного файла который хотим сохранить, бакет нэйм)
def upload_to_bucket(blob_name, path_to_file, bucket_name):
    """ Upload data to a bucket"""
    # Explicitly use service account credentials by specifying the private key file.
    storage_client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_JSON)
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(path_to_file)
    print('blob.public_url', blob.public_url)
    return blob.public_url

            # # # Все функции вызывать последовательно, по одной (остальные закоменчивать).
if __name__ == '__main__':
            # # # ( copy /путь к исходнику данных     /куда положить в PyCharm и как назвать файл )
    # os.system('cp temp/local_CSV_file.csv source_json_files/cinema_parsing_all_in_one.csv')

            # # #    (куда сохранить данные и как назвать файл, откуда исходник, имя_баккета)
    upload_to_bucket('write_csv_to_gc_bq/cinema_parsing_all_in_one.csv', 'source_json_files/cinema_parsing_all_in_one.csv', 'wr_short_bucket')

