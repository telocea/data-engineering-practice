import requests
import os
import zipfile
import io
import aiohttp
import asyncio

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]


def main():
    # your code here
    # create directory 'downloads'
    downloads_folder = 'downloads'
    os.makedirs(downloads_folder, exist_ok=True)

    # download files
    for uri in download_uris:
        r = requests.get(uri)
        if r.status_code == 200:
            extract_to = downloads_folder
            with zipfile.ZipFile(io.BytesIO(r.content)) as zip_ref:
                zip_ref.extractall(extract_to)
        else:
            print("Invalid URI: " + uri)



if __name__ == "__main__":
    main()
