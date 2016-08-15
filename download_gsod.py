"""
Downloads a single GSOD year, untars and unzips assets.
"""

import os.path
import gzip
import requests
import tarfile
from time import sleep

root_url = 'http://www1.ncdc.noaa.gov/pub/data'


def download_isd_history(save_dir):
    print "now downloading isd-history.csv"
    url = root_url + '/noaa/isd-history.csv'
    r = requests.get(url)
    with open(os.path.join(save_dir, 'isd-history.csv'), 'w') as f:
        f.write(r.content)


def download_isd_inventory(save_dir):
    print "now downloading isd-inventory.csv"
    url = root_url + '/noaa/isd-inventory.csv'
    r = requests.get(url)
    with open(os.path.join(save_dir, 'isd-inventory.csv'), 'w') as f:
        f.write(r.content)


def unpack(url):
    if url.find('op.gz') == -1:
        return None

    with gzip.open(url, 'r') as f:
        data = f.read()
    with open(url.rstrip('.gz'), 'w+') as f:
        f.write(data)
    os.remove(url)


def download_gsod_yr(yr, save_dir):
    print "now checking for tar file for "+str(yr)
    if not os.path.exists(os.path.join(save_dir, str(yr))):
        os.mkdir(os.path.join(save_dir, str(yr)))
    tar_url = root_url+'/gsod/'+str(yr)+'/'+'gsod_'+str(yr)+'.tar'
    tar_file = os.path.join(save_dir, 'gsod_' + str(yr) + '.tar')
    if not os.path.exists(tar_file):
        print 'tar file did not exist, downloading'
        r = requests.get(tar_url)
        with open(tar_file, 'w+') as f:
            f.write(r.content)
    tar = tarfile.open(tar_file)
    tar.extractall(path=os.path.join(save_dir, str(yr)))
    data_files_in_dir = os.listdir(os.path.join(save_dir, str(yr)))
    data_files_in_dir = [os.path.join(save_dir, str(yr), fname) for fname in data_files_in_dir]
    for url in data_files_in_dir:
        try:
            unpack(url)
        except:
            sleep(5)
            print("Error unpacking "+url+", retrying")
            unpack(url)
