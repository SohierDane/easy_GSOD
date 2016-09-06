"""
Update the GSOD data on S3 with the latest NOAA files.

TODO: add logging.
"""

import pandas as pd
import boto3
import botocore
import sys
from copy import deepcopy
from StringIO import StringIO
from time import sleep
from clean_and_export_op_file import raw_op_to_clean_dataframe
from clean_and_export_op_file import load_isd_history
from clean_and_export_op_file import robust_get_from_NOAA_ftp
from clean_and_export_op_file import get_station_year_inventory


root_gsod_url = 'http://www1.ncdc.noaa.gov/pub/data/gsod/'


def get_yrs_data_available():
    """
    Scrape NOAA's website to check what years have data available.
    Return a dataframe of the years available and when they were last modified.
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


def load_isd_inventory(bucket_name):
    """
    Load the isd_inventory into a dataframe if it already exists.
    If it doesn't exist, download it from NOAA.
    """
    s3 = boto3.resource('s3')
    try:
        inventory = StringIO(s3.Object(bucket_name, 'isd-inventory.csv')
                             .get()['Body'].read())
        is_from_NOAA = False
    except botocore.exceptions.ClientError:
        # Get the current isd-inventory from NOAA's ftp server
        inventory = robust_get_from_NOAA_ftp(
            '/pub/data/noaa/', 'isd-inventory.csv')
        is_from_NOAA = True
    inventory = pd.read_csv(
        inventory, dtype={col: str for col in ['ID', 'USAF', 'WBAN', 'YEAR']})
    if is_from_NOAA:
        """"
        Add new columns & initialize download records to date
        before NOAA ftp server existed
        """
        inventory.insert(0, 'ID', inventory['USAF']+'-'+inventory['WBAN'])
        inventory['Station-Year'] = inventory['ID']+'-'+inventory['YEAR']
        inventory['Last_Updated'] = pd.to_datetime(0)
    else:
        inventory['Last_Updated'] = pd.to_datetime(inventory['Last_Updated'])
    inventory.set_index('Station-Year', inplace=True)
    # ensure columns are organized properly
    cols = ['ID', 'USAF', 'WBAN', 'YEAR', 'Last_Updated', 'JAN', 'FEB',
            'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG',
            'SEP', 'OCT', 'NOV', 'DEC']
    return inventory[cols]


def df_to_csv_on_s3(df, bucket_name, key, csv_copy_index):
    s3 = boto3.resource('s3')
    f_buffer = StringIO()
    df.to_csv(f_buffer, index=csv_copy_index)
    f_buffer.seek(0)
    s3.Object(bucket_name, key).put(Body=f_buffer)


def get_years_to_check(bucket_name):
    """
    Return a list of all years for which the NOAA server has more
    current data than is stored locally, and the local annual download log.
    If no download log exists, it is created.
    """
    current_yrs_data = get_yrs_data_available()
    s3 = boto3.resource('s3')
    # if the annual logfile doesn't exist, create one
    try:
        annual_logs = StringIO(s3.Object(bucket_name,
            'annual_update_log.csv').get()['Body'].read())
        annual_logs = pd.read_csv(annual_logs, index_col='Year',
            parse_dates=['Modified'], infer_datetime_format=True)
    except botocore.exceptions.ClientError:
        annual_logs = deepcopy(current_yrs_data)
        annual_logs['Modified'] = pd.to_datetime(0)
        df_to_csv_on_s3(annual_logs, bucket_name, 'annual_update_log.csv', True)
    years_to_check = current_yrs_data[
        current_yrs_data['Modified'] > annual_logs['Modified']].index.values
    return [int(yr) for yr in years_to_check], annual_logs


def identify_files_on_NOAA_server_for_year(year):
    NOAA_url = 'http://www1.ncdc.noaa.gov/pub/data/gsod/'+str(year)+'/'
    NOAA_files = pd.read_html(NOAA_url, header=1)[0]
    NOAA_files.columns = ['a', 'File', 'Modified', 'b', 'c']
    NOAA_files = NOAA_files[['File', 'Modified']]
    NOAA_files['Modified'] = pd.to_datetime(NOAA_files['Modified'])
    NOAA_files['ID'] = NOAA_files['File'].apply(lambda x: x[:x.rfind('-')])
    NOAA_files = NOAA_files[NOAA_files['File'] != 'gsod_'+str(year)+'.tar']
    return NOAA_files


def get_stations_to_update_for_year(year, inventory, bucket):
    NOAA_files = identify_files_on_NOAA_server_for_year(year)
    s3 = boto3.resource('s3')
    files_on_s3 = [obj.key for obj in bucket.objects.filter(
        Prefix=str(year)+'/')]
    for key in files_on_s3:
        station_ID = key[key.find('/')+1:key.rfind('.')]
        if station_ID not in NOAA_files.ID.values:
            # remove any obsolete files on s3
            s3.Object(bucket.name, key).delete()
    # drop rows that are in the same year but not in NOAA files
    inventory = inventory[~(inventory.YEAR == str(year)) |
                          inventory.ID.isin(NOAA_files.ID)]
    files_to_update = NOAA_files.merge(inventory[inventory.YEAR == str(year)],
                                       how='left', on='ID')
    files_to_update = files_to_update[
        files_to_update['Modified'] > files_to_update['Last_Updated']]
    return inventory, files_to_update


def update_year(year, inventory, bucket, metadata):
    """
    Downloads any files that have more recent versions on NOAA's server
    than on S3, updates the inventory accordingly.
    """
    print "Now updating "+str(year)
    inventory, files_to_update = get_stations_to_update_for_year(
        year, inventory, bucket)
    year_url = root_gsod_url+str(year)+'/'
    download_counter = 0
    for station in files_to_update['File'].values:
        station_url = year_url+station
        df = raw_op_to_clean_dataframe(station_url, metadata)
        df_inventory = get_station_year_inventory(df)
        inventory = inventory[inventory.index.values != df_inventory.index[0]]
        inventory = inventory.append(df_inventory)
        key = str(year)+'/'+df.ID.iloc[0]+'.csv'
        df_to_csv_on_s3(df, bucket.name, key, False)
        download_counter += 1
        if download_counter % 500 == 0:
            print "Downloaded "+str(download_counter)+" files in "+str(year)
    return inventory


def update_metadata(metadata, inventory):
    """
    Drop stations that are in NOAA's metadata but not actually on the FTP
    server and add stations that were found on the FTP but not in the metadta.
    """
    metadata = metadata[metadata.index.isin(inventory.ID)]
    extra_stns = inventory[
        ~inventory.ID.isin(metadata.index)][['ID', 'USAF', 'WBAN']]
    extra_stns.drop_duplicates(subset='ID', inplace=True)
    extra_stns = extra_stns.reindex(columns=metadata.columns)
    return pd.concat([extra_stns, metadata], ignore_index=True)


def update_GSOD(bucket_name):
    inventory = load_isd_inventory(bucket_name)
    years_to_check, annual_logs = get_years_to_check(bucket_name)
    print('Preparing to update the following years:\n'+str(years_to_check))
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    metadata = load_isd_history()
    for year in years_to_check:
        inventory = update_year(year, inventory, bucket, metadata)
        df_to_csv_on_s3(inventory, bucket_name, 'isd-inventory.csv', True)
        annual_logs.Modified.loc[year] = pd.datetime.today()
        df_to_csv_on_s3(
            annual_logs, bucket_name, 'annual_update_log.csv', True)
        print("Logs updated for "+str(year))
    update_metadata(metadata, inventory, bucket_name)
    df_to_csv_on_s3(metadata, bucket_name, 'isd-history.csv', True)


def run_GSOD_update_daily(bucket_name):
    """
    Repeat the update once per day, indefinitely.
    """
    seconds_per_day = 60*60*24
    while True:
        update_GSOD(bucket_name)
        print "GSOD updated "+str(pd.datetime.today())
        sleep(seconds_per_day)


if __name__ == '__main__':
    update_GSOD(sys.argv[1])
