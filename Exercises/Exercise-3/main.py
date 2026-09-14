import boto3
import gzip
import os
import io


def main():
    # download file
    uri = ''
    s3 = boto3.client('s3')
    bucket = 'commoncrawl'
    key = 'crawl-data/CC-MAIN-2022-05/wet.paths.gz'

    response = s3.get_object(Bucket=bucket, Key=key)
    body = response['Body'].read()
    # extract file
    with gzip.open(io.BytesIO(body), 'rt') as f:
        uri = f.readline().strip()

    # download and store file
    response = s3.get_object(Bucket=bucket, Key=uri)
    body = response['Body'].read()

    # print each line of the file
    with gzip.open(io.BytesIO(body), 'rt') as f:
        for line in f:
            print(line.strip())


if __name__ == "__main__":
    main()
    
