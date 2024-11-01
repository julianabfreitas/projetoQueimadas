import requests
import zipfile
import json
from bs4 import BeautifulSoup
from io import BytesIO
from minio import Minio
from datetime import datetime
from pyspark.sql.functions import max

def get_url_content(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.content  
    except requests.exceptions.RequestException as err:
        print(f"Failed to retrieve content from {url}. Error: {err}")
        raise 

def get_links_by_extension(url, response_text, extension):
    soup = BeautifulSoup(response_text, 'html.parser')
    return [url + a['href'] for a in soup.find_all('a', href=True) if a['href'].endswith(extension)]

def upload_file_to_minio(file_data, file_length, minio_client, bucket_name, object_name):
    minio_client.put_object(
        bucket_name,
        object_name,
        file_data,
        length=file_length
    )

def unzip_upload_to_minio(zip_content, minio_client, bucket_name, prefix):
    with zipfile.ZipFile(BytesIO(zip_content)) as zfile:
        for file_name in zfile.namelist():
            with zfile.open(file_name) as file_data:
                upload_file_to_minio(file_data, zfile.getinfo(file_name).file_size, minio_client, bucket_name, f'{prefix}/{file_name}')

def update_log(df_spark, col_date, minio_client, bucket_name, path):
    last_upload = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    last_date = df_spark.agg(max(df_spark[col_date])).collect()[0][0]
    last_date = last_date.strftime("%Y-%m-%d %H:%M:%S")

    data = {
        "last_upload": last_upload,
        "last_date": last_date
    }

    json_data = json.dumps(data, indent=4).encode('utf-8')
    
    upload_file_to_minio(BytesIO(json_data), len(json_data), minio_client, bucket_name, path)
