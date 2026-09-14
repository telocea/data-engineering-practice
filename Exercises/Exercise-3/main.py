import boto3


def main():
    # download file
    s3 = boto3.client('s3')
    bucket = 'commoncrawl'
    key = 'crawl-data/CC-MAIN-2022-05/wet.paths.gz'

    s3.download_file(bucket, key, 'wet.paths.gz')
    # extract file

    # download and store file

    # print each line of the file
    # your code here
    pass


if __name__ == "__main__":
    main()
