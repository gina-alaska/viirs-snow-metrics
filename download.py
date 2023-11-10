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
import logging
from requests.auth import HTTPBasicAuth
from statistics import mean
from xml.etree import ElementTree as ET

from luts import short_name
from config import viirs_params, INPUT_DIR

# Class API session
# version
# is active
# start / stop functions handle is_active state
# make request


def check_data_version(ds_short_name):
    """Get latest version number of the dataset."""
    cmr_collections_url = "https://cmr.earthdata.nasa.gov/search/collections.json"
    response = requests.get(cmr_collections_url, params={"short_name": ds_short_name})
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
    logging.info(response)
    return session


def search_granules(ds_latest_version, ds_short_name, tstart, tstop, bbox):
    """Search for granules within a time range and geographic bounding box. API reference: https://cmr.earthdata.nasa.gov/search/site/docs/search/api.html#query-parameters"""

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
        if len(results["feed"]["entry"]) == 0:
            # Out of results, so break out of loop
            break

        # Collect results and increment page_num
        granules.extend(results["feed"]["entry"])
        search_params["page_num"] += 1

    logging.info(
        f"{len(granules)} granules of {ds_short_name} version {ds_latest_version} cover your area and time of interest."
    )

    granule_sizes = [float(granule["granule_size"]) for granule in granules]

    logging.info(
        f"The average size of each granule is {mean(granule_sizes):.2f} MB and the total size of all {len(granules)} granules is {sum(granule_sizes):.2f} MB"
    )
    # CP commenting out because this is well, very granular
    # logging.info(results)
    return granules


def set_n_orders_and_mode_and_page_size(granules):
    """Determine number of orders (endpoints) required based on number of granules requested. Requests will nearly always be async with the maximum page size (2000) but smaller dev chunks may benefit from synchronous requests and smaller page sizes."""

    def set_request_mode(granules):
        """Request data asynchronously or synchronously based on number of granules. The API has some built in limits on synchronous requests."""
        if len(granules) > 100:
            request_mode = "async"
        else:
            request_mode = "stream"
        return request_mode

    def set_page_size(granules):
        """Set page size (granule chunk size) for an individual API request."""
        if len(granules) > 100:
            page_size = 2000
        else:
            page_size = 100
        return page_size

    page_size = set_page_size(granules)
    page_num = math.ceil(len(granules) / page_size)
    request_mode = set_request_mode(granules)
    return page_num, request_mode, page_size


def construct_request(
    ds_latest_version, ds_short_name, tstart, tstop, bbox, req_mode, pg_size
):
    """Construct the API Download Request."""
    base_url = "https://n5eil02u.ecs.nsidc.org/egi/request"

    dl_param_dict = {
        "short_name": ds_short_name,
        "version": ds_latest_version,
        "temporal": f"{tstart},{tstop}",
        "bounding_box": bbox,
        "format": "GeoTIFF",  # NetCDF4-CF also viable, though failed to return data in an early test
        "projection": "GEOGRAPHIC",
        "page_size": pg_size,
        "request_mode": req_mode,
        "agent": "",  # may need to retain empty string value
    }

    # Convert to string: could use urllib quote - but will follow NSIDC example
    dl_string = "&".join("{!s}={!r}".format(k, v) for (k, v) in dl_param_dict.items())
    dl_string = dl_string.replace("'", "")

    # Construct request URL(s)
    endpoint_list = []
    for i in range(page_num):
        page_val = i + 1
        api_request = f"{base_url}?{dl_string}&page_num={page_val}"
        endpoint_list.append(api_request)

    logging.info(*endpoint_list)
    # we may not end up needing the list of endpoints, as the ordering function can use the dl_param_dict to construct the orders, in that case we wouldn't return them, but log them for reference or sharing
    return endpoint_list, dl_param_dict


def wipe_old_downloads():
    # probably want to wipe previously downloaded data if there
    # check nws-drought repo maybe for example
    # maybe ask user to confirm
    return None


def make_async_data_orders(n_orders, session, dl_param_dict):
    # make the download 'orders' because the API will need to verify the order and do some processing before making it ready for download
    # we have to pass an authenticated session to the scope of this function

    base_url = "https://n5eil02u.ecs.nsidc.org/egi/request"
    # Request data service for each page number i.e. order
    for i in range(n_orders):
        page_val = i + 1
        logging.info(f"Order: {page_val}")

        dl_param_dict["page_num"] = page_val
        request = session.get(base_url, params=dl_param_dict)
        logging.info(f"Request HTTP response: {request.status_code}")

        # raise bad request: Loop will stop for bad response code.
        request.raise_for_status()
        logging.info(f"Order request URL: {request.url}")
        esir_root = ET.fromstring(request.content)
        logging.info(f"Order request response XML content: {request.content}")

        # Look up order ID
        orderlist = []
        for order in esir_root.findall("./order/"):
            orderlist.append(order.text)
        orderID = orderlist[0]
        logging.info(f"order ID: {orderID}")

        # Create status URL
        statusURL = base_url + "/" + orderID
        logging.info(f"status URL: {statusURL}")

        # Find order status
        request_response = session.get(statusURL)
        logging.info(
            f"Status code from order response URL is {request_response.status_code}"
        )

        # Raise bad request: Loop will stop for bad response code.
        request_response.raise_for_status()
        request_root = ET.fromstring(request_response.content)
        statuslist = []
        for status in request_root.findall("./requestStatus/"):
            statuslist.append(status.text)
        status = statuslist[0]
        logging.info(f"Data request {page_val} is submitting...")
        logging.info(f"Initial request status is {status}")

        # Continue loop while request is still processing
        while status == "pending" or status == "processing":
            logging.info("Status is not complete. Trying again.")
            time.sleep(30)  # emit status twice per minute
            loop_response = session.get(statusURL)

            # Raise bad request: Loop will stop for bad response code.
            loop_response.raise_for_status()
            loop_root = ET.fromstring(loop_response.content)

            # find status
            statuslist = []
            for status in loop_root.findall("./requestStatus/"):
                statuslist.append(status.text)
            status = statuslist[0]
            print(status)
            logging.info(f"Retry request status is {status}")
            if status == "pending" or status == "processing":
                continue

        # Order can either complete, complete_with_errors, or fail:
        if status == "complete_with_errors" or status == "failed":
            logging.error("Order error messages:")
            for message in loop_root.findall("./processInfo/"):
                logging.error(message)

        # Download zipped order if status is complete or complete_with_errors
        if status == "complete" or status == "complete_with_errors":
            download_url = "https://n5eil02u.ecs.nsidc.org/esir/" + orderID + ".zip"
            logging.info(f"Zip download URL: {download_url}")
            return download_url
        else:
            logging.error("Request failed.")


# def download_order(download_url):
#         logging.info("Beginning download of zipped output...")
#         zip_response = session.get(download_url)
#         # Raise bad request: Loop will stop for bad response code.
#         zip_response.raise_for_status()
#         with zipfile.ZipFile(io.BytesIO(zip_response.content)) as z:
#             z.extractall(path)
#         logging.info("Data request", page_val, "is complete.")  # log this


if __name__ == "__main__":
    logging.basicConfig(filename="download.log", level=logging.INFO)
    v = check_data_version(short_name)
    api_session = start_api_session(short_name, v)
    # bail if session fails
    granule_list = search_granules(
        v,
        short_name,
        viirs_params["start_date"],
        viirs_params["end_date"],
        viirs_params["bbox"],
    )
    page_num, request_mode, page_size = set_n_orders_and_mode_and_page_size(
        granule_list
    )
    api_request, dl_params = construct_request(
        v,
        short_name,
        viirs_params["start_date"],
        viirs_params["end_date"],
        viirs_params["bbox"],
        request_mode,
        page_size,
    )
    if request_mode == "async":
        dl_url = make_async_data_orders(page_num, api_session, dl_params)
        print(dl_url)
    else:
        print("streaming mode")
    print("Download Script Complete")
