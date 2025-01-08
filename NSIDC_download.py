#!/usr/bin/env python
# ----------------------------------------------------------------------------
# NSIDC Data Download Script
#
# Copyright (c) 2025 Regents of the University of Colorado
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
#
# The script will first search Earthdata for all matching files.
# You will then be prompted for your Earthdata username/password
# and the script will download the matching files.
#
# If you wish, you may store your Earthdata username/password in a .netrc
# file in your $HOME directory and the script will automatically attempt to
# read this file. The .netrc file should have the following format:
#    machine urs.earthdata.nasa.gov login MYUSERNAME password MYPASSWORD
# where 'MYUSERNAME' and 'MYPASSWORD' are your Earthdata credentials.
#
# Instead of a username/password, you may use an Earthdata bearer token.
# To construct a bearer token, log into Earthdata and choose "Generate Token".
# To use the token, when the script prompts for your username,
# just press Return (Enter). You will then be prompted for your token.
# You can store your bearer token in the .netrc file in the following format:
#    machine urs.earthdata.nasa.gov login token password MYBEARERTOKEN
# where 'MYBEARERTOKEN' is your Earthdata bearer token.
#
# from __future__ import print_function

import base64
import getopt
import itertools
import json
import math
import netrc
import os.path
import ssl
import sys
import time
from getpass import getpass
import argparse
from urllib.parse import urlparse
from urllib.request import urlopen, Request, build_opener, HTTPCookieProcessor
from urllib.error import HTTPError, URLError

CMR_URL = "https://cmr.earthdata.nasa.gov"
URS_URL = "https://urs.earthdata.nasa.gov"
CMR_PAGE_SIZE = 2000
CMR_FILE_URL = (
    "{0}/search/granules.json?"
    "&sort_key[]=start_date&sort_key[]=producer_granule_id"
    "&page_size={1}".format(CMR_URL, CMR_PAGE_SIZE)
)
CMR_COLLECTIONS_URL = "{0}/search/collections.json?".format(CMR_URL)
# Maximum number of times to re-try downloading a file if something goes wrong.
FILE_DOWNLOAD_MAX_RETRIES = 3


def get_username():
    username = ""
    username = input("Earthdata username (or press Return to use a bearer token): ")
    return username


def get_password():
    password = ""
    while not password:
        password = getpass("password: ")
    return password


def get_token():
    token = ""
    while not token:
        token = getpass("bearer token: ")
    return token


def get_login_credentials():
    """Get user credentials from .netrc or prompt for input."""
    credentials = None
    token = None

    try:
        info = netrc.netrc()
        username, account, password = info.authenticators(urlparse(URS_URL).hostname)
        if username == "token":
            token = password
        else:
            credentials = "{0}:{1}".format(username, password)
            credentials = base64.b64encode(credentials.encode("ascii")).decode("ascii")
    except Exception:
        username = None
        password = None

    if not username:
        username = get_username()
        if len(username):
            password = get_password()
            credentials = "{0}:{1}".format(username, password)
            credentials = base64.b64encode(credentials.encode("ascii")).decode("ascii")
        else:
            token = get_token()

    return credentials, token


def build_version_query_params(version: str) -> str:
    desired_pad_length = 3
    if len(version) > desired_pad_length:
        print('Version string too long: "{0}"'.format(version))
        quit()

    version = str(int(version))  # Strip off any leading zeros
    query_params = ""

    while len(version) <= desired_pad_length:
        padded_version = version.zfill(desired_pad_length)
        query_params += "&version={0}".format(padded_version)
        desired_pad_length -= 1
    return query_params


def filter_add_wildcards(filter):
    if not filter.startswith("*"):
        filter = "*" + filter
    if not filter.endswith("*"):
        filter = filter + "*"
    return filter


def build_filename_filter(filename_filter):
    filters = filename_filter.split(",")
    result = "&options[producer_granule_id][pattern]=true"
    for filter in filters:
        result += "&producer_granule_id[]=" + filter_add_wildcards(filter)
    return result


def build_query_params_str(
    short_name,
    version,
    time_start="",
    time_end="",
    bounding_box=None,
    polygon=None,
    filename_filter=None,
    provider=None,
):
    """Create the query params string for the given inputs.

    E.g.,: '&short_name=ATL06&version=006&version=06&version=6'
    """
    params = "&short_name={0}".format(short_name)
    if version:
        params += build_version_query_params(version)
    if time_start or time_end:
        # See
        # https://cmr.earthdata.nasa.gov/search/site/docs/search/api.html#temporal-range-searches
        params += "&temporal[]={0},{1}".format(time_start, time_end)
    if polygon:
        params += "&polygon={0}".format(polygon)
    elif bounding_box:
        params += "&bounding_box={0}".format(bounding_box)
    if filename_filter:
        params += build_filename_filter(filename_filter)
    if provider:
        params += "&provider={0}".format(provider)

    return params


def build_cmr_query_url(
    short_name,
    version,
    time_start,
    time_end,
    bounding_box=None,
    polygon=None,
    filename_filter=None,
    provider=None,
):
    params = build_query_params_str(
        short_name=short_name,
        version=version,
        time_start=time_start,
        time_end=time_end,
        bounding_box=bounding_box,
        polygon=polygon,
        filename_filter=filename_filter,
        provider=provider,
    )

    return CMR_FILE_URL + params


def get_speed(time_elapsed, chunk_size):
    if time_elapsed <= 0:
        return ""
    speed = chunk_size / time_elapsed
    if speed <= 0:
        speed = 1
    size_name = ("", "k", "M", "G", "T", "P", "E", "Z", "Y")
    i = int(math.floor(math.log(speed, 1000)))
    p = math.pow(1000, i)
    return "{0:.1f}{1}B/s".format(speed / p, size_name[i])


def output_progress(count, total, status="", bar_len=60):
    if total <= 0:
        return
    fraction = min(max(count / float(total), 0), 1)
    filled_len = int(round(bar_len * fraction))
    percents = int(round(100.0 * fraction))
    bar = "=" * filled_len + " " * (bar_len - filled_len)
    fmt = "  [{0}] {1:3d}%  {2}   ".format(bar, percents, status)
    print("\b" * (len(fmt) + 4), end="")  # clears the line
    sys.stdout.write(fmt)
    sys.stdout.flush()


def cmr_read_in_chunks(file_object, chunk_size=1024 * 1024):
    """Read a file in chunks using a generator. Default chunk size: 1Mb."""
    while True:
        data = file_object.read(chunk_size)
        if not data:
            break
        yield data


def get_login_response(url, credentials, token):
    opener = build_opener(HTTPCookieProcessor())

    req = Request(url)
    if token:
        req.add_header("Authorization", "Bearer {0}".format(token))
    elif credentials:
        try:
            response = opener.open(req)
            # We have a redirect URL - try again with authorization.
            url = response.url
        except HTTPError:
            # No redirect - just try again with authorization.
            pass
        except Exception as e:
            print("Error{0}: {1}".format(type(e), str(e)))
            sys.exit(1)

        req = Request(url)
        req.add_header("Authorization", "Basic {0}".format(credentials))

    try:
        response = opener.open(req)
    except HTTPError as e:
        err = "HTTP error {0}, {1}".format(e.code, e.reason)
        if "Unauthorized" in e.reason:
            if token:
                err += ": Check your bearer token"
            else:
                err += ": Check your username and password"
            print(err)
            sys.exit(1)
        raise
    except Exception as e:
        print("Error{0}: {1}".format(type(e), str(e)))
        sys.exit(1)

    return response


def cmr_download(urls, force=False, quiet=False, download_dir="."):
    """Download files from list of urls."""
    if not urls:
        return

    url_count = len(urls)
    if not quiet:
        print("Downloading {0} files...".format(url_count))
    credentials = None
    token = None

    for index, url in enumerate(urls, start=1):
        if not credentials and not token:
            p = urlparse(url)
            if p.scheme == "https":
                credentials, token = get_login_credentials()

        filename = url.split("/")[-1]
        file_path = os.path.join(download_dir, filename)
        if not quiet:
            print(
                "{0}/{1}: {2}".format(
                    str(index).zfill(len(str(url_count))), url_count, filename
                )
            )

        for download_attempt_number in range(1, FILE_DOWNLOAD_MAX_RETRIES + 1):
            if not quiet and download_attempt_number > 1:
                print("Retrying download of {0}".format(url))
            try:
                response = get_login_response(url, credentials, token)
                length = int(response.headers["content-length"])
                try:
                    if not force and length == os.path.getsize(file_path):
                        if not quiet:
                            print("  File exists, skipping")
                        # We have already downloaded the file. Break out of the
                        # retry loop.
                        break
                except OSError:
                    pass
                count = 0
                chunk_size = min(max(length, 1), 1024 * 1024)
                max_chunks = int(math.ceil(length / chunk_size))
                time_initial = time.time()
                with open(file_path, "wb") as out_file:
                    for data in cmr_read_in_chunks(response, chunk_size=chunk_size):
                        out_file.write(data)
                        if not quiet:
                            count = count + 1
                            time_elapsed = time.time() - time_initial
                            download_speed = get_speed(time_elapsed, count * chunk_size)
                            output_progress(count, max_chunks, status=download_speed)
                if not quiet:
                    print()
                # If we get here, the download was successful and we can break
                # out of the retry loop.
                break
            except HTTPError as e:
                print("HTTP error {0}, {1}".format(e.code, e.reason))
            except URLError as e:
                print("URL error: {0}".format(e.reason))
            except IOError:
                raise

            # If this happens, none of our attempts to download the file
            # succeeded. Print an error message and raise an error.
            if download_attempt_number == FILE_DOWNLOAD_MAX_RETRIES:
                print("failed to download file {0}.".format(filename))
                sys.exit(1)


def cmr_filter_urls(search_results):
    """Select only the desired data files from CMR response."""
    if "feed" not in search_results or "entry" not in search_results["feed"]:
        return []

    entries = [e["links"] for e in search_results["feed"]["entry"] if "links" in e]
    # Flatten "entries" to a simple list of links
    links = list(itertools.chain(*entries))

    urls = []
    unique_filenames = set()
    for link in links:
        if "href" not in link:
            # Exclude links with nothing to download
            continue
        if "inherited" in link and link["inherited"] is True:
            # Why are we excluding these links?
            continue
        if "rel" in link and "data#" not in link["rel"]:
            # Exclude links which are not classified by CMR as "data" or "metadata"
            continue

        if "title" in link and "opendap" in link["title"].lower():
            # Exclude OPeNDAP links--they are responsible for many duplicates
            # This is a hack; when the metadata is updated to properly identify
            # non-datapool links, we should be able to do this in a non-hack way
            continue

        filename = link["href"].split("/")[-1]

        if "metadata#" in link["rel"] and filename.endswith(".dmrpp"):
            # Exclude .dmrpp metadata links that exist in cloud-hosted
            # collections
            continue

        if filename in unique_filenames:
            # Exclude links with duplicate filenames (they would overwrite)
            continue
        unique_filenames.add(filename)

        urls.append(link["href"])

    return urls


def get_max_version(entries):
    v = max(entry["version_id"] for entry in entries)
    return v


def check_provider_for_collection(short_name, provider, version=None):
    """Return `True` if the collection is available for the given provider, otherwise `False`."""

    query_params = build_query_params_str(
        short_name=short_name, version=version, provider=provider
    )
    cmr_query_url = CMR_COLLECTIONS_URL + query_params

    req = Request(cmr_query_url)
    try:
        # TODO: context w/ ssl stuff here?
        response = urlopen(req)
    except Exception as e:
        print("Error: " + str(e))
        sys.exit(1)

    search_page = response.read()
    search_page = json.loads(search_page.decode("utf-8"))

    if "feed" not in search_page or "entry" not in search_page["feed"]:
        return False, version

    if len(search_page["feed"]["entry"]) > 0:
        if not version:
            version = get_max_version(search_page["feed"]["entry"])
        return True, version

    else:
        return False, version


def get_provider_for_collection(short_name, version):
    """Return the provider for the collection associated with the given short_name and version.

    Cloud-hosted data (NSIDC_CPRD) is preferred, but some datasets are still
    only available in ECS. Eventually all datasets will be hosted in the
    cloud. ECS is planned to be decommissioned in July 2026.
    """
    cloud_provider = "NSIDC_CPRD"
    in_earthdata_cloud, version = check_provider_for_collection(
        short_name, cloud_provider, version
    )
    if in_earthdata_cloud:
        return cloud_provider, version

    ecs_provider = "NSIDC_ECS"
    in_ecs, version = check_provider_for_collection(short_name, ecs_provider, version)
    if in_ecs:
        return ecs_provider, version

    raise RuntimeError(
        "Found no collection matching the given short_name ({0}) and version ({1})".format(
            short_name, version
        )
    )


def cmr_search(
    short_name,
    time_start,
    time_end,
    version=None,
    bounding_box="",
    polygon="",
    filename_filter="",
    quiet=False,
):
    """Perform a scrolling CMR query for files matching input criteria."""
    provider, version = get_provider_for_collection(
        short_name=short_name, version=version
    )
    cmr_query_url = build_cmr_query_url(
        provider=provider,
        short_name=short_name,
        version=version,
        time_start=time_start,
        time_end=time_end,
        bounding_box=bounding_box,
        polygon=polygon,
        filename_filter=filename_filter,
    )
    if not quiet:
        print("Querying for data:\n\t{0}\n".format(cmr_query_url))

    cmr_paging_header = "cmr-search-after"
    cmr_page_id = None
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    urls = []
    hits = 0
    while True:
        req = Request(cmr_query_url)
        if cmr_page_id:
            req.add_header(cmr_paging_header, cmr_page_id)
        try:
            response = urlopen(req, context=ctx)
        except Exception as e:
            print("Error: " + str(e))
            sys.exit(1)

        # Python 2 and 3 have different case for the http headers
        headers = {k.lower(): v for k, v in dict(response.info()).items()}
        if not cmr_page_id:
            # Number of hits is on the first result set, which will not have a
            # page id.
            hits = int(headers["cmr-hits"])
            if not quiet:
                if hits > 0:
                    print("Found {0} matches.".format(hits))
                else:
                    print("Found no matches.")

        # If there are multiple pages, we'll get a new page ID on each request.
        cmr_page_id = headers.get(cmr_paging_header)

        search_page = response.read()
        search_page = json.loads(search_page.decode("utf-8"))
        url_scroll_results = cmr_filter_urls(search_page)
        if not url_scroll_results:
            break
        if not quiet and hits > CMR_PAGE_SIZE:
            print(".", end="")
            sys.stdout.flush()
        urls += url_scroll_results

    if not quiet and hits > CMR_PAGE_SIZE:
        print()
    return urls


def search_and_download(
    short_name,
    time_start,
    time_end,
    bounding_box,
    version=None,
    polygon="",
    filename_filter="",
    quiet=False,
    force=False,
    url_list=[],
    download_dir=".",
):
    try:
        if not url_list:
            url_list = cmr_search(
                short_name,
                time_start,
                time_end,
                version=version,
                bounding_box=bounding_box,
                polygon=polygon,
                filename_filter=filename_filter,
                quiet=quiet,
            )
        cmr_download(url_list, force=force, quiet=quiet, download_dir=download_dir)
    except KeyboardInterrupt:
        quit()


def main(argv=None):

    parser = argparse.ArgumentParser(description="Download script for NSIDC data.")

    parser.add_argument("-f", "--force", action="store_true", help="Force execution")
    parser.add_argument("-q", "--quiet", action="store_true", help="Run in quiet mode")
    parser.add_argument(
        "--short_name", type=str, default="VNP10A1F", help="NASA dataset short name."
    )
    parser.add_argument(
        "--version",
        type=str,
        default=None,
        help="If entered, specifies version. Default will look for higheest version number.",
    )
    parser.add_argument(
        "--time_start",
        type=str,
        default="2023-08-01T00:00:00Z",
        help="Specify start time (Default: 2023-08-01T00:00:00Z)",
    )
    parser.add_argument(
        "--time_end",
        type=str,
        default="2023-08-02T23:59:59Z",
        help="Specify end time (Default: 2023-08-02T23:59:59Z)",
    )
    parser.add_argument(
        "--bounding_box",
        type=str,
        default="-150,60,-145,65",
        help="Specify bounding box (Default: -150,60,-145,65)",
    )
    parser.add_argument("--polygon", type=str, help="Specify polygon as WKT")
    parser.add_argument(
        "--filename_filter", type=str, help="Specify filename filter (e.g., '*.hdf')"
    )
    parser.add_argument(
        "--download_dir", type=str, default=".", help="Directory for downloaded files."
    )

    args = parser.parse_args()

    bounding_box = "-150,60,-145,65"
    polygon = ""
    filename_filter = ""
    url_list = []

    short_name = args.short_name
    version = args.version
    time_start = args.time_start
    time_end = args.time_end
    bounding_box = args.bounding_box
    polygon = args.polygon
    filename_filter = args.filename_filter
    force = args.force
    quiet = args.quiet
    download_dir = args.download_dir

    search_and_download(
        short_name,
        time_start,
        time_end,
        bounding_box,
        version,
        polygon,
        filename_filter,
        quiet,
        force,
        url_list,
        download_dir,
    )


if __name__ == "__main__":
    main()
