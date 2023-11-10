"""Download VIIRS Data from the NSIDC DAAC"""

import requests
import getpass
import socket
import json
import zipfile
import io
import math
import os
import shutil
import pprint
import re
import time
from requests.auth import HTTPBasicAuth
from statistics import mean

from luts import short_name
from config import viirs_params

# Class API session
# version
# is active
# start / stop functions handle is_active state
# make request


def check_data_version(ds_short_name):
    """Get latest version number of the dataset."""
    cmr_collections_url = "https://cmr.earthdata.nasa.gov/search/collections.json"
    response = requests.get(cmr_collections_url, params={"short_name": short_name})
    results = json.loads(response.content)

    # find all instances of 'version_id' in metadata
    versions = [el["version_id"] for el in results["feed"]["entry"]]
    latest_version = max(versions)
    return latest_version


def start_api_session(ds_short_name, ds_latest_version):
    """Initate an authenticated NSDIC DAAC API session."""
    uid = input("Earthdata Login user name: ")
    pswd = getpass.getpass("Earthdata Login password: ")

    capability_url = f"https://n5eil02u.ecs.nsidc.org/egi/capabilities/{ds_short_name}.{ds_latest_version}.xml"

    # Create session to store cookie and pass credentials to capabilities url
    session = requests.session()
    s = session.get(capability_url)
    response = session.get(s.url, auth=(uid, pswd))
    return response


def search_granules(ds_latest_version, ds_short_name, tstart, tstop, bbox):
    """Search for granules within a time range and geographic bounding box. API reference: https://cmr.earthdata.nasa.gov/search/site/docs/search/api.html#query-parameters"""
    # Create CMR parameters used for granule search. Modify params depending on bounding_box or polygon input.

    granule_search_url = "https://cmr.earthdata.nasa.gov/search/granules"
    search_params = {
        "short_name": ds_short_name,
        "version": ds_latest_version,
        "temporal": f"{tstart},{tstop}",
        "page_size": 100,
        "page_num": 1,
        "bounding_box": bbox,
    }

    granules = []
    headers = {"Accept": "application/json"}
    while True:
        response = requests.get(
            granule_search_url, params=search_params, headers=headers
        )
        results = json.loads(response.content)
        print(results)
        if len(results["feed"]["entry"]) == 0:
            # Out of results, so break out of loop
            break

        # Collect results and increment page_num
        granules.extend(results["feed"]["entry"])
        search_params["page_num"] += 1

    print(
        f"{len(granules)} granules of {ds_short_name} version {ds_latest_version} cover your area and time of interest."
    )

    granule_sizes = [float(granule["granule_size"]) for granule in granules]

    print(
        f"The average size of each granule is {mean(granule_sizes):.2f} MB and the total size of all {len(granules)} granules is {sum(granule_sizes):.2f} MB"
    )


if __name__ == "__main__":
    v = check_data_version(short_name)
    r = start_api_session(short_name, v)
    print(r)
    search_granules(
        v,
        short_name,
        viirs_params["start_date"],
        viirs_params["end_date"],
        viirs_params["bbox"],
    )

    base_request_url = f"https://n5eil02u.ecs.nsidc.org/egi/request?short_name=VNP10A1F&version=2&temporal=2013-01-01T00%3A00%3A00Z%2C2014-12-31T00%3A00%3A00Z&bounding_box=-146%2C64%2C-144%2C66&bbox=-146%2C64%2C-144%2C66&format=GeoTIFF&projection=GEOGRAPHIC&page_size=2000&request_mode=async&page_num=1"
