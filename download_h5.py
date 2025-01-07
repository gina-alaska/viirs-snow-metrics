"""Download VIIRS Data from the NSIDC DAAC. See https://github.com/nsidc/NSIDC-Data-Access-Notebook for reference. This module uses large amounts of code from notebook examples within that repo."""

import requests
import getpass
import json
import zipfile
import io
import math
import os
import shutil
import re
import sys
import time
import logging
import calendar
import argparse
from statistics import mean
from xml.etree import ElementTree as ET
from datetime import datetime, timedelta

from luts import short_name
from config import viirs_params, snow_year_input_dir, SNOW_YEAR
from NSIDC_download import search_and_download

from download import wipe_old_downloads, generate_monthly_dl_chunks

if __name__ == "__main__":
    log_file_path = os.path.join(os.path.expanduser("~"), "input_data_download.log")
    logging.basicConfig(filename=log_file_path, level=logging.INFO)

    parser = argparse.ArgumentParser(description="Download Script - HDF5")
    parser.add_argument("--short_name", type=str, help="Dataset short name - will overwrite short_name from luts if used.")
    args = parser.parse_args()
    if args.short_name:
        short_name = args.short_name

    wipe_old_downloads(snow_year_input_dir)

    #v = check_data_version(short_name)
    #ref_tiles = determine_tiles_for_bbox(viirs_params["bbox"])
    snow_year_chunks = generate_monthly_dl_chunks(int(SNOW_YEAR))

    #api_session = start_api_session(short_name, v)

    for time_chunk in snow_year_chunks:
        print(f"Starting download for {time_chunk}.")
        print(short_name, viirs_params, snow_year_input_dir)
        search_and_download(short_name, time_chunk[0], time_chunk[1], viirs_params["bbox"], download_dir=snow_year_input_dir)
        #granule_list = search_granules(
        #    v,
        #    short_name,
        #    time_chunk[0],
        #    time_chunk[1],
        #    viirs_params["bbox"],
        #)
        #filtered_granules = filter_granules_based_on_tiles(granule_list, ref_tiles)

        #page_num, request_mode, page_size = set_n_orders_and_mode_and_page_size(
        #    filtered_granules
        #)
        #api_request, dl_params = construct_request(
        #    v,
        #    short_name,
        #    time_chunk[0],
        #    time_chunk[1],
        #    viirs_params["bbox"],
        #    request_mode,
        #    page_size,
        #    page_num,
        #)

        #dl_urls = make_async_data_orders(page_num, api_session, dl_params)
        #download_order(api_session, dl_urls, snow_year_input_dir)
        #flatten_download_directory(snow_year_input_dir)
        #validate_download(snow_year_input_dir, len(granule_list))

    #api_session.close()
    print("Download Script Complete.")
