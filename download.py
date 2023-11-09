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

from luts import short_name


# def start_api_session():
#     """This function initates as NSDIC DAAC API session."""
    

#     capability_url = f'https://n5eil02u.ecs.nsidc.org/egi/capabilities/{short_name}.{latest_version}.xml'

#     # Create session to store cookie and pass credentials to capabilities url
#     session = requests.session()
#     s = session.get(capability_url)
#     response = session.get(s.url,auth=(uid,pswd))
#     return response

if __name__ == "__main__":
    # Earthdata credentials
    uid = input("Earthdata Login user name: ")
    pswd = getpass.getpass("Earthdata Login password: ")
    #start_api_session()

    base_request_url = f"https://n5eil02u.ecs.nsidc.org/egi/request?short_name=VNP10A1F&version=2&temporal=2013-01-01T00%3A00%3A00Z%2C2014-12-31T00%3A00%3A00Z&bounding_box=-146%2C64%2C-144%2C66&bbox=-146%2C64%2C-144%2C66&format=GeoTIFF&projection=GEOGRAPHIC&page_size=2000&request_mode=async&page_num=1"