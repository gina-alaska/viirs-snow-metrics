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
from statistics import mean
from xml.etree import ElementTree as ET
from datetime import datetime, timedelta

from luts import short_name, needed_tile_ids
from config import viirs_params, snow_year_input_dir, SNOW_YEAR


def wipe_old_downloads(dl_path):
    """Convenience function to prompt user to wipe prior downloads but retain the target directory. The baseline assumption is that all data in `$INPUT_DIR/$SNOW_YEAR` maps to a single cohesive processing run for a single snow year and set of tiles.

    Args:
        dl_path (pathlib.Path): The path to the download directory.

    Returns:
        None
    """
    with os.scandir(dl_path) as item:
        if any(item):
            print(f"The target path {dl_path} for the download is not empty.")
            user_input = input(
                "Wipe the contents of the target download directory? (y/n): "
            ).lower()
            if user_input == "y":
                for filename in os.listdir(dl_path):
                    filepath = os.path.join(dl_path, filename)
                    try:
                        shutil.rmtree(filepath)
                    except OSError:
                        os.remove(filepath)
                print(f"Target path {dl_path} is now empty.")
            else:
                print(
                    f"Proceeding with target directory {dl_path} that already contains data."
                )
        else:
            print(f"The target path {dl_path} for the download is empty.")
    return None


def check_data_version(ds_short_name):
    """Get latest version number of the dataset. In general, we'll always want to download the latest version of the data and so this should help us in the future. MODIS has seen numerous version updates, would not be surprised if VIIRS does too in the future.
    Args:
        ds_short_name (str): The short name of the dataset. This is the unique "special" NSIDC alphanumeric identifier for the dataset and is used in the CMR metadata search.

    Returns:
        int: The latest version number.
    """
    cmr_collections_url = "https://cmr.earthdata.nasa.gov/search/collections.json"
    response = requests.get(cmr_collections_url, params={"short_name": ds_short_name})
    results = json.loads(response.content)

    # find all instances of 'version_id' in metadata
    versions = [el["version_id"] for el in results["feed"]["entry"]]
    latest_version = max(versions)
    return latest_version


def generate_monthly_dl_chunks(snow_year):
    """Generate monthly time intervals for downloading data. A snow year is defined as August 1 to July 31 to this function helps get the correct months lined up with the correct year.

    Args:
        snow_year (int): The snow year of interest.
    Returns:
        list: A list of monthly time intervals.
    """
    intervals = []
    for month in range(1, 13):
        year = snow_year - 1 if month >= 8 else snow_year
        start_date = datetime(year, month, 1)
        _, last_day = calendar.monthrange(year, month)
        end_date = datetime(year, month, last_day, 23, 59, 59)

        interval = (
            start_date.strftime("%Y-%m-%dT00:00:00Z"),
            end_date.strftime("%Y-%m-%dT23:59:59Z"),
        )

        intervals.append(interval)
    return intervals


def start_api_session(ds_short_name, ds_latest_version):
    """Initate an authenticated NSDIC DAAC API session.

    Args:
        ds_short_name (str): The short name of the dataset.
        ds_latest_version (int): The latest version number of the dataset.

    Returns:
        requests.sessions.Session: A session with authenticated access to the NSIDC API.

    """
    uid = input("Earthdata Login user name: ")
    pswd = getpass.getpass("Earthdata Login password: ")

    capability_url = f"https://n5eil02u.ecs.nsidc.org/egi/capabilities/{ds_short_name}.{ds_latest_version}.xml"

    # Create session to store cookie and pass credentials to capabilities url
    session = requests.session()
    s = session.get(capability_url)
    response = session.get(s.url, auth=(uid, pswd))
    if response.status_code != 200:
        # bail
        auth_fail_msg = f"API authentication failed with status code {response.status_code}, exiting program."
        print(auth_fail_msg)
        logging.error(auth_fail_msg)
        sys.exit(1)
    else:
        # pass the session on
        auth_pass_msg = f"{response.status_code}, API authentication successful."
        print(auth_pass_msg)
        logging.info(auth_pass_msg)
        return session


def search_granules(ds_latest_version, ds_short_name, tstart, tstop, bbox):
    """Search for granules within a time range and geographic bounding box. API reference: https://cmr.earthdata.nasa.gov/search/site/docs/search/api.html#query-parameters

    Args:
        ds_latest_version (int): The latest version number of the dataset.
        ds_short_name (str): The short name of the dataset.
        tstart (str): Start of temporal range (e.g., "2022-01-01T00:00:00Z").
        tstop (str): End of temporal range (e.g., "2022-12-31T23:59:59Z").
        bbox (str): Bounding box in the format "min_lon,min_lat,max_lon,max_lat".

    Returns:
        list: A list of granules matching the specified criteria.
    """
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
            # out of results, break out of loop
            break
        # collect results and increment page_num
        granules.extend(results["feed"]["entry"])
        search_params["page_num"] += 1

    logging.info(
        f"{len(granules)} granules of {ds_short_name} version {ds_latest_version} cover your area and time of interest."
    )
    granule_sizes = [float(granule["granule_size"]) for granule in granules]
    logging.info(
        f"The average size of each granule is {mean(granule_sizes):.2f} MB and the total size of all {len(granules)} granules is {sum(granule_sizes):.2f} MB"
    )
    # CP note: results is low-level meta for granules, log for debugging
    logging.info(results)
    return granules


def filter_granules_based_on_tiles(granules, ref):
    """Filter granules based on a list of VIIRS sinusoidal grid tiles know to match the spatial domain.

    Args:
        granules (list): A list of candidate granules.
        ref (set): A set of reference tiles.
    Returns:
        list: A list of granules that match the reference tiles.
    """
    filtered = [x for x in granules if x["producer_granule_id"].split(".")[2] in ref]
    logging.info(f"After filtering {len(filtered)} granules remain.")

    granule_sizes = [float(granule["granule_size"]) for granule in filtered]
    logging.info(
        f"The average size of each filtered granule is {mean(granule_sizes):.2f} MB and the total size of all filtered {len(filtered)} granules is {sum(granule_sizes):.2f} MB"
    )
    return filtered


def set_n_orders_and_mode_and_page_size(granules):
    """Determine number of orders (endpoints) required based on number of granules requested. Requests will nearly always be async with the maximum page size (2000) but smaller dev chunks may benefit from synchronous requests and smaller page sizes. In testing a "stream" request mode hung, so for now this function is hard coded to return "async" for the request mode.

    Args:
        granules (list): A list of granules.

    Returns:
        tuple: containing number of orders (int), request_mode "async" or "stream" (str), and the page size for the API request (int).
    """

    def set_page_size(granules):
        """Set page size (granule chunk size) for an individual API request.

        Args:
            granules (list): A list of granules.

        Returns:
            page_size (int): The page size for the API request.
        """
        if len(granules) > 100:
            page_size = 2000
        else:
            page_size = 100
        return page_size

    page_size = set_page_size(granules)
    n_pages = math.ceil(len(granules) / page_size)
    request_mode = "async"
    logging.info(f"Number of orders is {n_pages} with size {page_size}.")
    return n_pages, request_mode, page_size


def construct_request(
    ds_latest_version, ds_short_name, tstart, tstop, bbox, req_mode, pg_size, n_pages
):
    """Construct the API Download Requests.

    Args:
        ds_latest_version (str): The latest version of the dataset.
        ds_short_name (str): The short name of the dataset.
        tstart (str): The start of the temporal range.
        tstop (str): The end of the temporal range.
        bbox (str): The bounding box for the geographic area of interest.
        req_mode (str): request mode ("async" or "stream").
        pg_size (int): The page size for the API request.
        n_pages (int): Number of pages for API request (i.e., number of orders)

    Returns:
        tuple: containing a list of API endpoints and a dictionary of download parameters.
    """
    base_url = "https://n5eil02u.ecs.nsidc.org/egi/request"
    # this is the sauce where we ask NSIDC to do additional upstream processing
    dl_param_dict = {
        "short_name": ds_short_name,
        "version": ds_latest_version,
        "temporal": f"{tstart},{tstop}",
        "bounding_box": bbox,
        "format": "GeoTIFF",  # CP note: `NetCDF4-CF`` option failed to return data in test - though this may be a more efficient alternative
        "projection": "GEOGRAPHIC",  # CP note: this is nice!
        "page_size": pg_size,
        "request_mode": req_mode,
    }

    # CP note: could use urllib quote - but will follow NSIDC example of constructing the request string
    dl_string = "&".join("{!s}={!r}".format(k, v) for (k, v) in dl_param_dict.items())
    dl_string = dl_string.replace("'", "")

    # construct request URLs
    endpoint_list = []
    for i in range(n_pages):
        page_val = i + 1
        api_request = f"{base_url}?{dl_string}&page_num={page_val}"
        endpoint_list.append(api_request)
        logging.info(f"Request endpoint: {api_request}")
    # CP note: may not need the list of endpoints to download data because the ordering function can use the dl_param_dict to construct the orders as well. retain for logging and debugging though.
    return endpoint_list, dl_param_dict


def make_async_data_orders(n_orders, session, dl_param_dict):
    """Make the download orders. The API will need to verify the order and process and prepare it before making it ready available to download. An authenticated session must pass to the scope of this function.

    Args:
        n_orders (int): orders (pages) required based on the granules requested.
        session (requests.sessions.Session): authenticated API session.
        dl_param_dict (dict): dictionary of download parameters.

    Returns:
        download_urls (list): URLs for downloading the ordered data.
    """
    base_url = "https://n5eil02u.ecs.nsidc.org/egi/request"
    # Request data service for each page number i.e. order
    download_urls = []
    logging.info(f"There are {n_orders} orders (number of pages).")

    for i in range(n_orders):
        page_val = i + 1
        logging.info(f"Async Data Order: {page_val}")

        dl_param_dict["page_num"] = page_val
        request = session.get(base_url, params=dl_param_dict)
        logging.info(f"Request HTTP response: {request.status_code}")

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
            time.sleep(3600)  # emit status every hour
            loop_response = session.get(statusURL)
            loop_response.raise_for_status()
            loop_root = ET.fromstring(loop_response.content)

            # find status
            statuslist = []
            for status in loop_root.findall("./requestStatus/"):
                statuslist.append(status.text)
            status = statuslist[0]
            print(f"data order is {status}")
            logging.info(f"Retry request status is {status}")
            if status == "pending" or status == "processing":
                continue

        # Order can either complete, complete with errors, or fail
        # CP note: not really sure what "complete with error" means
        if status == "complete_with_errors" or status == "failed":
            logging.error("Order error messages:")
            for message in loop_root.findall("./processInfo/"):
                logging.error(message)

        if status == "complete":
            dl_url = "https://n5eil02u.ecs.nsidc.org/esir/" + orderID + ".zip"
            logging.info(f"Zip download URL for order {orderID}: {dl_url}")
            download_urls.append(dl_url)
        else:
            logging.error("Request failed.")
    return download_urls


def download_order(session, download_urls, dl_path):
    """Download and extract the ordered data.

    Args:
        session (requests.sessions.Session): authenticated session for making requests.
        download_urls (list): URL(s) for downloading the ordered data.
        dl_path (pathlib.Path): target directory for downloading and extracting data

    Returns:
        None
    """

    for dl_url in download_urls:
        logging.info("Beginning download of zipped output...")
        zip_response = session.get(dl_url)
        # CP note: hacky retry loop, but did once get a "service unavailable" status when the request URL itself was valid. try 3x before giving up.
        try:
            for _ in range(3):
                zip_response = session.get(dl_url)
                if zip_response.status_code == 200:
                    break
                time.sleep(120)  # Pause for 2 minutes
            zip_response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print(f"Error downloading zip file: {e}")
        with zipfile.ZipFile(io.BytesIO(zip_response.content)) as z:
            z.extractall(dl_path)
    logging.info("Data request is complete.")


def flatten_download_directory(dl_path):
    """Clean up downloads by removing individual granule folders and by moving files from nested directories to the root download directory. Goal is to have a single directory of input data for processing, and use file names to identify the types of data within each file.

    Args:
        dl_path (pathlib.Path): The directory path of the download.

    Returns:
        None
    """
    logging.info("Flattening data from nested directories...")
    for root, dirs, files in os.walk(dl_path, topdown=False):
        for file in files:
            try:
                shutil.move(os.path.join(root, file), dl_path)
            except OSError:
                pass
        for name in dirs:
            os.rmdir(os.path.join(root, name))
    logging.info(f"{dl_path} now a flat directory.")


def validate_download(dl_path, number_tiles_requested, days_requested, n_variables=1):
    """Validate the number of HDF5 files downloaded against the number of tiles expected.

    Days requested is passed to this function to allow for months or years with differing lengths as input.

    Args:
        dl_path (pathlib.Path): The directory path of the download.
        number_tiles_requested (int): The number of unique tiles requested.
        days_requested (int): The number of days requested (i.e., days in month or year).
        n_variables (int): The number of variables expected per tile per day. Default to 1, use 5 for GeoTIFF downloads.

    Returns:
        None
    """
    dl_file_count = sum(1 for x in dl_path.rglob("*") if x.is_file())
    dl_files_expected = number_tiles_requested * days_requested * n_variables
    if dl_file_count != dl_files_expected:
        logging.warning(
            f"{dl_file_count} files were downloaded, but based on {number_tiles_requested} granules a downloaded file count of {dl_files_expected} is expected."
        )
    else:
        logging.info("Downloaded file count matches expectations.")


def download_tif():
    """Wrapper function to download GeoTIFF files. Uses methods for conversion and reprojection that will not be available for future datasets.
    These methods are retained for legacy support and may cease to function properly in the future.

    Args:
        None

    Returns:
        None
    """
    v = check_data_version(short_name)
    snow_year_chunks = generate_monthly_dl_chunks(int(SNOW_YEAR))

    api_session = start_api_session(short_name, v)

    for time_chunk in snow_year_chunks:
        logging.info(f"Starting download for {time_chunk}.")
        granule_list = search_granules(
            v,
            short_name,
            time_chunk[0],
            time_chunk[1],
            viirs_params["bbox"],
        )
        filtered_granules = filter_granules_based_on_tiles(
            granule_list, needed_tile_ids
        )

        page_num, request_mode, page_size = set_n_orders_and_mode_and_page_size(
            filtered_granules
        )
        api_request, dl_params = construct_request(
            v,
            short_name,
            time_chunk[0],
            time_chunk[1],
            viirs_params["bbox"],
            request_mode,
            page_size,
            page_num,
        )

        dl_urls = make_async_data_orders(page_num, api_session, dl_params)
        download_order(api_session, dl_urls, snow_year_input_dir)
        flatten_download_directory(snow_year_input_dir)
        # Parse month from time_chunk and get number of days in the month
        start_date = datetime.strptime(time_chunk[0], "%Y-%m-%dT%H:%M:%SZ")
        month = start_date.month
        _, days_in_month = calendar.monthrange(int(SNOW_YEAR), month)
        validate_download(
            snow_year_input_dir, len(needed_tile_ids), days_in_month, n_variables=5
        )

    days_in_year = 366 if calendar.isleap(int(SNOW_YEAR)) else 365
    validate_download(
        snow_year_input_dir, len(needed_tile_ids), days_in_year, n_variables=5
    )
    api_session.close()


def download_h5(short_name=short_name):
    """Wrapper function to download HDF5 files from the NSIDC DAAC using earthaccess API.
    This is the preferred method for downloading VIIRS data.

    Args:
        short_name (str): The short name of the dataset. Default is stored in luts.py.

    Returns:
        None
    """
    import earthaccess

    earthaccess.login(strategy="interactive")
    snow_year_chunks = generate_monthly_dl_chunks(int(SNOW_YEAR))

    for time_chunk in snow_year_chunks:
        logging.info(f"Starting download for {time_chunk}.")
        datasets = earthaccess.search_datasets(
            short_name=short_name,
        )
        if len(datasets) == 1:
            version = int(datasets[0]["umm"]["Version"])
        else:
            versions = [
                int(ds["umm"]["Version"])
                for ds in datasets
                if "umm" in ds and "Version" in ds["umm"]
            ]
            version = max(versions) if versions else None
        url_list = earthaccess.search_data(
            short_name=short_name,
            bounding_box=tuple(map(int, viirs_params["bbox"].split(","))),
            temporal=(time_chunk[0], time_chunk[1]),
            # daac='NSIDC', # Seems to work without this - but possible specifying daac is needed to avoid duplicates for some years/data
            version=int(version),
        )
        if url_list:
            # Filter to only needed tiles
            url_list = [
                url
                for url in url_list
                for tile_id in needed_tile_ids
                if tile_id in url["umm"]["GranuleUR"]
            ]
            logging.info(
                f"{len(url_list)} files match needed tiles for {time_chunk}. Downloading..."
            )
            if url_list:
                earthaccess.download(url_list, local_path=snow_year_input_dir)
            else:
                logging.info(f"No files match needed tiles for {time_chunk}. Exiting.")
                exit(1)

            # Parse month from time_chunk and get number of days in the month
            start_date = datetime.strptime(time_chunk[0], "%Y-%m-%dT%H:%M:%SZ")
            month = start_date.month
            _, days_in_month = calendar.monthrange(int(SNOW_YEAR), month)
            validate_download(snow_year_input_dir, len(needed_tile_ids), days_in_month)

        else:
            logging.info(f"No files found for {time_chunk}. Exiting.")
            exit(1)

    days_in_year = 366 if calendar.isleap(int(SNOW_YEAR)) else 365
    validate_download(snow_year_input_dir, len(needed_tile_ids), days_in_year)


if __name__ == "__main__":
    import argparse

    log_file_path = os.path.join(os.path.expanduser("~"), "input_data_download.log")
    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(message)s",
        filename=log_file_path,
        level=logging.INFO,
    )
    parser = argparse.ArgumentParser(description="Download Script.")

    parser.add_argument(
        "--format",
        "-f",
        choices=["tif", "h5"],
        default="h5",
        help="Download/input File format: Older processing methods use tif, newer uses h5. Default is h5.",
    )
    parser.add_argument(
        "--short_name",
        type=str,
        help="Dataset short name - will overwrite short_name from luts if used. Only usable with h5 download methods.",
    )
    args = parser.parse_args()
    if args.short_name:
        short_name = args.short_name

    wipe_old_downloads(snow_year_input_dir)

    if args.format == "tif":
        download_tif()
    else:
        download_h5(short_name=short_name)

    print("Download Script Complete.")
