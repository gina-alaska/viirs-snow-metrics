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
from pathlib import Path
import earthaccess

from luts import short_name, needed_tile_ids
from config import viirs_params, snow_year_input_dir, SNOW_YEAR

from download import wipe_old_downloads, generate_monthly_dl_chunks


def main(short_name):
    wipe_old_downloads(snow_year_input_dir)
    snow_year_chunks = generate_monthly_dl_chunks(int(SNOW_YEAR))

    for time_chunk in snow_year_chunks:
        print(f"Starting download for {time_chunk}.")
        url_list = earthaccess.search_data(
            short_name=short_name,
            bounding_box=tuple(map(int, viirs_params["bbox"].split(","))),
            temporal=(time_chunk[0], time_chunk[1]),
            # daac='NSIDC', # Seems to work without this - but possible specifying daac is needed to avoid duplicates for some years/data
            version=2,
        )
        earthaccess.download(url_list, local_path=snow_year_input_dir)


if __name__ == "__main__":
    log_file_path = os.path.join(os.path.expanduser("~"), "input_data_download.log")
    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(message)s",
        filename=log_file_path,
        level=logging.INFO,
    )

    parser = argparse.ArgumentParser(description="Download Script - HDF5")
    parser.add_argument(
        "--short_name",
        type=str,
        help="Dataset short name - will overwrite short_name from luts if used.",
    )
    args = parser.parse_args()
    if args.short_name:
        short_name = args.short_name

    main(short_name)

    print("Download Script Complete.")
