"""
Downloads a single GSOD year, untars and unzips assets.

TODO: add log of which files were downloaded when so can update only those
updated by NOAA. Will also need option to switch between bulk download
(large .tar files) and piecemeal (small .gz files)
"""

import os.path
import pandas as pd
import gzip
import requests
import tarfile
import boto3
import botocore
from ftplib import FTP
from copy import deepcopy
from StringIO import StringIO
from time import sleep


root_gsod_url = 'http://www1.ncdc.noaa.gov/pub/data/gsod/'


def get_yrs_data_available():
    """
    Scrape NOAA's website to check what years have data available.

    Returns a dataframe of the years available and when they were last modified
    """
    global root_gsod_url
    df = pd.read_html(root_gsod_url, header=1)[0]
    df.columns = ['a', 'Year', 'Modified', 'b', 'c']
    df = df[['Year', 'Modified']]
    # trim the trailing slash sign
    df['Year'] = df['Year'].apply(lambda x: x[:len(x)-1])
    # strip table entries which aren't years (such as the readme file)
    df = df[df['Year'].apply(len) == 4]
    df['Modified'] = pd.to_datetime(df['Modified'])
    df.set_index('Year', inplace=True)
    return df


def load_station_metadata():
    """
    Load the isd-history metadata file directly from the NOAA server.
    Intent of rebuilding S3 copy from scratch every time is to take
    advantage of any new data NOAA uploads.
    """
    ftp = FTP('ftp.ncdc.noaa.gov')
    ftp.login()
    ftp.cwd('/pub/data/noaa/')
    metadata = StringIO()
    ftp.retrbinary('RETR isd-history.csv', metadata.write)
    metadata.seek(0)
    metadata_df = pd.read_csv(metadata,
        dtype={col: str for col in ['USAF', 'WBAN', 'BEGIN', 'END', 'STATION NAME']})
    metadata_df['ID'] = metadata_df['USAF']+'-'+metadata_df['WBAN']
    metadata_df.set_index(['ID'])
    metadata_df = clean_history_metadata(metadata_df)
    metadata_df = unpack_date_info(metadata_df, 'BEGIN', 'Begin_')
    metadata_df = unpack_date_info(metadata_df, 'END', 'End_')
    return metadata_df


def load_isd_inventory(bucket_name):
    """
    Load the isd_inventory into a dataframe if it already exists.
    If it doesn't exist, download it from NOAA.
    """
    s3 = boto3.resource('s3')
    try:
        inventory = (s3.Object(bucket_name, 'isd-inventory.csv')
                     .get()['Body'].read())
    except botocore.exceptions.ClientError:
        # Get the current isd-inventory from NOAA's ftp server
        ftp = FTP('ftp.ncdc.noaa.gov')
        ftp.login()
        ftp.cwd('/pub/data/noaa/')
        inventory = StringIO()
        ftp.retrbinary('RETR isd-inventory.csv', inventory.write)
        inventory.seek(0)
    inventory = pd.read_csv(
        inventory, dtype={col: str for col in ['USAF', 'WBAN']})
    if 'ID' not in inventory.columns:
        inventory.insert(0, 'ID', inventory['USAF']+'-'+inventory['WBAN'])
    if 'Last_Updated' not in inventory.columns:
        # initialize download records to date before NOAA ftp server existed
        inventory['Last_Updated'] = pd.to_datetime(0)
    return inventory


def unpack(url):
    if url.find('op.gz') == -1:
        return None

    with gzip.open(url, 'r') as f:
        data = f.read()
    with open(url.rstrip('.gz'), 'w+') as f:
        f.write(data)
    os.remove(url)


def download_gsod_yr(yr, save_dir):
    global root_gsod_url
    print "now checking for tar file for "+str(yr)
    if not os.path.exists(os.path.join(save_dir, str(yr))):
        os.mkdir(os.path.join(save_dir, str(yr)))
    tar_url = root_gsod_url+'/gsod/'+str(yr)+'/'+'gsod_'+str(yr)+'.tar'
    tar_file = os.path.join(save_dir, 'gsod_' + str(yr) + '.tar')
    if not os.path.exists(tar_file):
        print 'tar file did not exist, downloading'
        r = requests.get(tar_url)
        with open(tar_file, 'w+') as f:
            f.write(r.content)
    tar = tarfile.open(tar_file)
    tar.extractall(path=os.path.join(save_dir, str(yr)))
    data_files_in_dir = os.listdir(os.path.join(save_dir, str(yr)))
    data_files_in_dir = [os.path.join(save_dir, str(yr), fname)
                         for fname in data_files_in_dir]
    for url in data_files_in_dir:
        try:
            unpack(url)
        except:
            sleep(5)
            print("Error unpacking "+url+", retrying")
            unpack(url)


def get_years_to_check(bucket_name):
    """
    Return a dataframe of all years for which the NOAA server has more
    current data than is stored locally. If no download log exists
    it is created.
    """
    current_yrs_data = get_yrs_data_available()
    s3 = boto3.resource('s3')
    # if the annual logfile doesn't exist, create one
    try:
        annual_logs = (s3.Object(bucket_name, 'year_update_log.csv')
                       .get()['Body'].read())
    except botocore.exceptions.ClientError:
        annual_logs = deepcopy(current_yrs_data)
        annual_logs['Modified'] = pd.datetime(0)
        f_buffer = StringIO()
        annual_logs.to_csv(f_buffer)
        f_buffer.seek(0)
        s3.Object(bucket_name, 'year_update_log.csv').put(Body=f_buffer)
    return current_yrs_data[
            current_yrs_data['Modified'] > annual_logs['Modified']]


def update_year(year):
    pass




def update_GSOD(bucket_name):
    inventory = load_isd_inventory(bucket_name)
    yrs_to_check = get_years_to_check()
    for year in yrs_to_check:
        update_year(year)
    update_metadata()
    
