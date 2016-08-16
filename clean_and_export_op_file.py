"""
Unpacks NOAA's .op format into a .csv, adds columns for station metadata,
provides more readable column names, replaces NOAA missing value codes with NaN
and provides separate columns for each data category.

Please see ftp://ftp.ncdc.noaa.gov/pub/data/gsod/readme.txt
for the original .op specifications.
"""

import pandas as pd
from numpy import nan


def reorganize_columns(df):
    """
    Reset the columns into a useful order.
    """
    header_cols = ['ID_Code', 'USAF_ID_Code', 'WBAN_ID_Code', 'Elevation', 'Country_Code',
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


def load_op_into_dataframe(raw_f_path):
    """
    Read an op file into a dataframe and adds basic headers
    """
    with open(raw_f_path, 'r') as f:
        # read one line to skip past the header
        f.readline()
        data = [line.strip().split() for line in f.readlines()]

    df = pd.DataFrame(data, dtype=str)
    df.columns = ['USAF_ID_Code', 'WBAN_ID_Code', 'yrmoda',
                   'Mean_Temp', 'Mean_Temp_Count', 'Mean_Dewpoint', 'Mean_Dewpoint_Count',
                   'Mean_Sea_Level_Pressure', 'Mean_Sea_Level_Pressure_Count',
                   'Mean_Station_Pressure', 'Mean_Station_Pressure_Count',
                   'Mean_Visibility', 'Mean_Visibility_Count', 'Mean_Windspeed',
                   'Mean_Windspeed_Count', 'Max_Windspeed', 'Max_Gust', 'Max_Temp',
                   'Min_Temp', 'Precipitation', 'Snow_Depth', 'FRSHTT']
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


def missing_codes_to_nan(df):
    """
    Replace NOAA's various missing data codes with nan.
    Treat the WBAN/USAF 99999 entries as valid.
    """
    columns_to_ignore = ['USAF_ID_Code', 'WBAN_ID_Code', 'ID_Code']
    columns_to_use = [x for x in df.columns if x not in columns_to_ignore]
    df[columns_to_use] = df[columns_to_use].replace(
        to_replace='9[.9]{3,4}9', value=nan, regex=True)
    return df


def get_metadata(station_ID, metadata_df, lookup_field):
    """
    Find the metadata for a station, if it exists.
    """
    if station_ID not in metadata_df.index:
        return nan
    else:
        return metadata_df[lookup_field].loc[station_ID]


def raw_op_to_clean_dataframe(raw_f_path, isd_history):
    """
    Take original NASA data, drops several columns,
    split date into more useful formats, and convert to csv format
    """
    df = load_op_into_dataframe(raw_f_path)
    df['ID_Code'] = df['USAF_ID_Code']+'-'+df['WBAN_ID_Code']
    df = unpack_FRSHTT(df)
    df = unpack_date_info(df)
    df = unpack_quality_flags(df)
    station_ID = df['ID_Code'].iloc[0]
    df['Elevation'] = get_metadata(station_ID, isd_history, 'ELEV(M)')
    df['Station_Name'] = get_metadata(station_ID, isd_history, 'STATION NAME')
    df['Country_Code'] = get_metadata(station_ID, isd_history, 'CTRY')
    df['Latitude'] = get_metadata(station_ID, isd_history, 'LAT')
    df['Longitude'] = get_metadata(station_ID, isd_history, 'LON')
    df = missing_codes_to_nan(df)
    df = reorganize_columns(df)
    return df


def inventory_stations_year(df):
    """"
    Generates a dataframe with counts of populated fields by month.

    Mirrors the format of one row of the isd-inventory, plus a unique
    identifier column.
    """
    idx = '-'.join(df[["ID_Code", "Year"]].iloc[0].values.tolist())
    col_names = ["JAN", "FEB", "MAR", "APR", "MAY",
                 "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
    data = df.groupby('Month').count().sum(axis=1).to_frame(idx).T
    data.columns = col_names
    data["USAF_ID_Code"] = df["USAF_ID_Code"].iloc[0]
    data["WBAN_ID_Code"] = df["WBAN_ID_Code"].iloc[0]
    data["Year"] = df["Year"].iloc[0]
    return data


def raw_op_to_clean_csv(raw_f_path, isd_history):
    """
    Export .op file to a cleaned .csv.

    Return station ID as success flag.
    """
    df = raw_op_to_clean_dataframe(raw_f_path, isd_history)
    df.to_csv(index=False)
    return df['ID_Code'].iloc[0]


def process_stations(stn_path_list, isd_history):
    """
    Unpack all op files in the list to .csv,
    inventory the data, return set of stations found

    TODO: complete function
    """
    pass


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


def load_isd_history(isd_path):
    """
    Load & clean the raw NOAA metadata file

    Expects the .csv version of the isd-history
    """
    metadata_df = pd.read_csv(isd_path,
        dtype={col: str for col in ['USAF', 'WBAN', 'BEGIN', 'END', 'STATION NAME']})
    metadata_df['ID'] = metadata_df['USAF']+'-'+metadata_df['WBAN']
    metadata_df.set_index(['ID'])
    metadata_df = clean_history_metadata(metadata_df)
    metadata_df = unpack_date_info(metadata_df, 'BEGIN', 'Begin_')
    metadata_df = unpack_date_info(metadata_df, 'END', 'End_')
    return metadata_df


def clean_inventory_metadata(df):
    """
    TODO: Should remove items that don't actually exist
    """
    return df


def load_isd_inventory(isd_path):
    """
    TODO: Add function to generate an accurate inventory.
    """
    pass
