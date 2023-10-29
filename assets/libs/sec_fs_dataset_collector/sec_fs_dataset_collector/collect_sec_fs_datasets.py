import requests
import tempfile
import zipfile
import boto3

def fetch_zip_file(url: str, destination: str, file_name: str):
    with open(destination+file_name, mode="wb") as file:
        response = requests.get(url, allow_redirects=True)
        file.write(response.content)

def decompress_zip_file(file_path: str, destination: str):
    with zipfile.ZipFile(file_path, mode="r") as archive:
        archive.extractall(destination)

def handle_quarter(year: str, quarter: str, s3_bucket: str, s3_prefix_dest: str, source_url: str):
    with tempfile.TemporaryDirectory() as tmp_dir:
        file_name = "{}q{}.zip".format(year, quarter)
        file_destination = tmp_dir+"/"
        fetch_zip_file(url=source_url+file_name, destination=file_destination, file_name=file_name)
        uncompressed_dest = file_destination+"uncompressed_files/"
        decompress_zip_file(file_path=file_destination+file_name, destination=uncompressed_dest)
        s3_client = boto3.Session().client('s3')
        s3_client.upload_file(uncompressed_dest+"num.txt", s3_bucket, s3_prefix_dest+"FS_NUM/{}q{}.txt".format(year, quarter))
        s3_client.upload_file(uncompressed_dest+"pre.txt", s3_bucket, s3_prefix_dest+"FS_PRE/{}q{}.txt".format(year, quarter))
        s3_client.upload_file(uncompressed_dest+"sub.txt", s3_bucket, s3_prefix_dest+"FS_SUB/{}q{}.txt".format(year, quarter))
        s3_client.upload_file(uncompressed_dest+"tag.txt", s3_bucket, s3_prefix_dest+"FS_TAG/tag.txt") # This file is like a dimension table, it has to be overwritten

