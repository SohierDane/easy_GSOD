"""
Unpacks NOAA's .op format into a .csv, adds columns for station metadata,
provides more readable column names, replaces NOAA missing value codes with NaN
and provides separate columns for each data category.

Please see ftp://ftp.ncdc.noaa.gov/pub/data/gsod/readme.txt
for the original .op specifications.
"""

import pandas as pd
import urllib
import gzip
from numpy import nan
from StringIO import StringIO
from time import sleep
from ftplib import FTP


def get_from_NOAA_ftp(dir, file):
    """
    Downloads a file from NOAA's ftp server into a file buffer
    """
    ftp = FTP('ftp.ncdc.noaa.gov')
    ftp.login()
    ftp.cwd('/pub/data/noaa/')
    file_buffer = StringIO()
    ftp.retrbinary('RETR '+file, file_buffer.write)
    file_buffer.seek(0)
    return file_buffer


def robust_get_from_NOAA_ftp(dir, file):
    max_attempts = 10
    for i in xrange(max_attempts):
        try:
            return get_from_NOAA_ftp(dir, file)
        except:
            sleep(10)
            print("Error accessing "+file+", retrying")
    # shouldn't get here
    raise Exception('Failed to Download'+file)


def robust_download(url):
    """
    Download to string buffer w/ multiple attempts
    """
    max_attempts = 10
    for i in xrange(max_attempts):
        try:
            file_obj = StringIO(urllib.urlopen(url).read())
            file_obj.seek(0)
            return file_obj
        except:
            sleep(10)
            print("Error accessing "+file+", retrying")
    # shouldn't get here
    raise Exception('Failed to Download'+file)


def missing_codes_to_nan(df):
    """
    Replace NOAA's various missing data codes with nan.
    Treat the WBAN/USAF 99999 entries as valid.
    """
    columns_to_ignore = ['USAF', 'WBAN', 'ID']
    columns_to_use = [x for x in df.columns if x not in columns_to_ignore]
    df[columns_to_use] = df[columns_to_use].replace(
        to_replace='9[.9]{3,4}9', value=nan, regex=True)
    return df


def load_op_into_dataframe(raw_data_path):
    """
    Load an op.gz file, unzip it in memory, read it into a dataframe,
    and adds basic headers.

    If path is a url, will download the file.
    """
    if 'http' == raw_data_path[:len('http')]:
        f = gzip.GzipFile(fileobj=robust_download(raw_data_path))
    else:
        f = open(raw_data_path, 'r')
    # readline to drop the unwanted original unwanted header
    f.readline()
    df = pd.read_csv(f, dtype=str, header=None, delim_whitespace=True, names=[
        'USAF', 'WBAN', 'yrmoda',
        'Mean_Temp', 'Mean_Temp_Count', 'Mean_Dewpoint', 'Mean_Dewpoint_Count',
        'Mean_Sea_Level_Pressure', 'Mean_Sea_Level_Pressure_Count',
        'Mean_Station_Pressure', 'Mean_Station_Pressure_Count',
        'Mean_Visibility', 'Mean_Visibility_Count', 'Mean_Windspeed',
        'Mean_Windspeed_Count', 'Max_Windspeed', 'Max_Gust', 'Max_Temp',
        'Min_Temp', 'Precipitation', 'Snow_Depth', 'FRSHTT'])
    f.close()
    return df


def unpack_FRSHTT(df):
    """
    Extract the 6 fog/rain/etc fields into separate columns.
    """
    col_names = ['Fog', 'Rain_or_Drizzle', 'Snow_or_Ice',
                 'Hail', 'Thunder', 'Tornado']
    for i, col_nm in enumerate(col_names):
        df[col_nm] = df['FRSHTT'].apply(lambda x: x[i])
    del df['FRSHTT']
    return df


def unpack_date_info(df, date_col_nm='yrmoda', prefix=''):
    '''
    Unpack NASA yrmoda string in to date, year, month, day

    For example:
    >>> extract_date_info('20020103')
    ['2002-01-03', '2002', '01', '03']
    '''
    df[prefix+'Year'] = df[date_col_nm].apply(lambda x: x[:4])
    df[prefix+'Month'] = df[date_col_nm].apply(lambda x: x[4:6])
    df[prefix+'Day'] = df[date_col_nm].apply(lambda x: x[6:])
    df[prefix+'Date'] = df[date_col_nm].apply(lambda x: '-'.join(
        [x[:4], x[4:6], x[6:]]))
    del df[date_col_nm]
    return df


def unpack_quality_flags(df):
    """
    Extract the data quality flags from the data columns.
    """
    for col in ['Max_Temp', 'Min_Temp']:
        df[col+'_Quality_Flag'] = df[col].apply(
            lambda x: 1 if x[-1] == '*' else 0)
        df[col] = df[col].apply(lambda x: str(x).rstrip('*'))
    df['Precip_Flag'] = df['Precipitation'].apply(lambda x: x[-1])
    df['Precipitation'] = df['Precipitation'].apply(lambda x: x[:-1])
    return df


def raw_op_to_clean_dataframe(raw_data_path, isd_history):
    """
    Take original NASA data, drop several columns,
    split date into more useful formats, and load into dataframe
    """
    df = load_op_into_dataframe(raw_data_path)
    df['ID'] = df['USAF']+'-'+df['WBAN']
    df = unpack_FRSHTT(df)
    df = unpack_date_info(df)
    df = unpack_quality_flags(df)
    station_ID = df['ID'].iloc[0]
    df['Elevation'] = get_metadata(station_ID, isd_history, 'ELEV(M)')
    df['Station_Name'] = get_metadata(station_ID, isd_history, 'STATION NAME')
    df['Country_Code'] = get_metadata(station_ID, isd_history, 'CTRY')
    df['Latitude'] = get_metadata(station_ID, isd_history, 'LAT')
    df['Longitude'] = get_metadata(station_ID, isd_history, 'LON')
    df = missing_codes_to_nan(df)
    df = reorganize_data_columns(df)
    return df


def get_station_year_inventory(df):
    """"
    Generate a dataframe with counts of primary weather fields by month.
    Ignores the count and min/max fields, precipitation type, and metadata.

    Output mirrors the format of one row of the isd-inventory, plus
    identifier columns and update logs. Assumes file was just updated if
    it is being inventoried.
    """
    idx = '-'.join(df[["ID", "Year"]].iloc[0].values.tolist())
    month_nums = [str(i).zfill(2) for i in range(1, 13)]
    """
    We build a dataframe initialized with zeroes to handle files
    with missing months.
    """
    data = pd.DataFrame(data=0, index=[idx], columns=month_nums)
    data.index.name = 'Station-Year'
    cols_to_inventory = ['Mean_Temp', 'Mean_Dewpoint',
                         'Mean_Sea_Level_Pressure', 'Mean_Station_Pressure',
                         'Mean_Visibility', 'Mean_Windspeed', 'Precipitation',
                         'Month']
    counts = (df[cols_to_inventory].groupby('Month')
              .count().sum(axis=1).to_frame(idx).T)
    data = data.add(counts, fill_value=0)
    data.columns = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN",
                    "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
    data = data.astype(int)
    data["USAF"] = df["USAF"].iloc[0]
    data["WBAN"] = df["WBAN"].iloc[0]
    data["ID"] = df["ID"].iloc[0]
    data["YEAR"] = df["Year"].iloc[0]
    data['Last_Updated'] = pd.datetime.today()
    return data


def raw_op_to_clean_csv(raw_data_path, isd_history):
    """
    Export .op file to a cleaned .csv.

    Return station ID as success flag.
    """
    df = raw_op_to_clean_dataframe(raw_data_path, isd_history)
    df.to_csv(index=False)
    return df['ID'].iloc[0]


def reorganize_data_columns(df):
    """
    Reset the columns into a useful order.
    """
    header_cols = ['ID', 'USAF', 'WBAN', 'Elevation', 'Country_Code',
                   'Latitude', 'Longitude', 'Date', 'Year', 'Month', 'Day',
                   'Mean_Temp', 'Mean_Temp_Count', 'Mean_Dewpoint', 'Mean_Dewpoint_Count',
                   'Mean_Sea_Level_Pressure', 'Mean_Sea_Level_Pressure_Count',
                   'Mean_Station_Pressure', 'Mean_Station_Pressure_Count',
                   'Mean_Visibility', 'Mean_Visibility_Count', 'Mean_Windspeed',
                   'Mean_Windspeed_Count', 'Max_Windspeed', 'Max_Gust', 'Max_Temp',
                   'Max_Temp_Quality_Flag', 'Min_Temp', 'Min_Temp_Quality_Flag',
                   'Precipitation', 'Precip_Flag', 'Snow_Depth', 'Fog',
                   'Rain_or_Drizzle', 'Snow_or_Ice', 'Hail', 'Thunder',
                   'Tornado']
    return df[header_cols]


def get_metadata(station_ID, metadata_df, lookup_field):
    """
    Find the metadata for a station, if it exists.
    """
    if station_ID not in metadata_df.index:
        return nan
    else:
        return metadata_df[lookup_field].loc[station_ID]


def clean_bogus_name(station_name):
    """
    Replace station names including 'bogus' with nan.

    Per the NOAA readme file, GSOD uses 'bogus' as a keyword showing
    that the station name is unknown.
    """
    if station_name is nan:
        return nan
    elif 'BOGUS' in station_name or 'UNKNOWN' in station_name:
        return nan
    else:
        return station_name


def clean_history_metadata(df):
    """
    Replace all unreasonable elevation, latitude, longitude values with nan.
    Replace all names that indicate a missing name with nan.
    Lowest real point on dry land is the border of the dead sea @ 418 M.
    https://en.wikipedia.org/wiki/Extreme_points_of_Earth#Lowest_point_.28natural.29
    """
    elevation_of_lowest_pt_on_dry_land = -418
    df['ELEV(M)'] = df['ELEV(M)'].apply(
        lambda x: x if x >= elevation_of_lowest_pt_on_dry_land else nan)
    max_possible_lat = 90
    min_possible_lat = -90
    max_possible_lon = 180
    min_possible_lon = -180
    df['LAT'] = df['LAT'].apply(lambda x:
        x if x > min_possible_lat and x < max_possible_lat else nan)
    df['LON'] = df['LON'].apply(lambda x:
        x if x > min_possible_lon and x < max_possible_lon else nan)
    invalid_names = ['NAME/LOCATION UNKN', 'NAME UNKNOWN (ONC)', 'APPROXIMATE LOCATIO',
                     'APPROXIMATE LOCALE', 'APPROXIMATE LOCATION', 'NAME AND LOC UNKN',
                     'NAME UNKNOWN', 'NAME0LOCATION UNKN', 'NAME\LOCATION UNKN']
    df['STATION NAME'].replace({nm: nan for nm in invalid_names}, inplace=True)
    df['STATION NAME'] = df['STATION NAME'].apply(clean_bogus_name)
    return df


def load_isd_history():
    """
    Load the isd-history metadata file directly from the NOAA server.
    Intent of reloading from scratch every time is to take
    advantage of any new data NOAA uploads.

    TODO: correct or delete the begin/end dates
    """
    metadata_df = pd.read_csv(
        robust_get_from_NOAA_ftp('/pub/data/noaa/', 'isd-history.csv'),
        dtype={col: str for col in ['USAF', 'WBAN', 'BEGIN', 'END', 'STATION NAME']})
    metadata_df['ID'] = metadata_df['USAF']+'-'+metadata_df['WBAN']
    metadata_df.set_index(['ID'])
    metadata_df = clean_history_metadata(metadata_df)
    metadata_df = unpack_date_info(metadata_df, 'BEGIN', 'Begin_')
    metadata_df = unpack_date_info(metadata_df, 'END', 'End_')
    return metadata_df
