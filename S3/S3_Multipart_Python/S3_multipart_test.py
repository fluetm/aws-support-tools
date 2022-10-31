import boto3
import os
import time

from boto3.s3.transfer import TransferConfig

s3_resource = boto3.resource('s3')
client = boto3.client('s3', 'us-east-1')
config = TransferConfig(multipart_threshold=1024 * 25,
                        max_concurrency=15,
                        multipart_chunksize=1024 * 25,
                        use_threads=False)
t = boto3.s3.transfer.S3Transfer(client=client, config=config)

bucket_name = 'mjf-vector-test'
upload_filename='matt-256mb.kdb'
s3_filename='mjf/destination-mp.kdb'

def multipart_upload_boto3():
    tic = time.perf_counter()
    t.upload_file(upload_filename, bucket_name, s3_filename)
    toc = time.perf_counter()
    print(f"Uploaded {upload_filename} in {config.multipart_chunksize/1024}mb chunks with {config.max_concurrency} max threads in {toc - tic:0.4f} seconds")

def multipart_download_boto3():
    #  MJF: This is a lot slower than the CLI
    file_path = os.path.dirname(__file__) + '/matt-download.kdb'
    tic = time.perf_counter()
    t.download_file(bucket_name, s3_filename, file_path)
    toc = time.perf_counter()
    print(f"Downloaded {s3_filename} in {config.multipart_chunksize/1024}mb chunks with {config.max_concurrency} max threads in {toc - tic:0.4f} seconds")

#multipart_upload_boto3()
multipart_download_boto3()