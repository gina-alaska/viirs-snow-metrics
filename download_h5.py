"""Download VIIRS Data from the NSIDC DAAC. See https://earthaccess.readthedocs.io/en/stable/ for reference."""

import os
import logging
import earthaccess

from luts import short_name, needed_tile_ids
from config import viirs_params, snow_year_input_dir, SNOW_YEAR

from download import wipe_old_downloads, generate_monthly_dl_chunks


def main(short_name):
    wipe_old_downloads(snow_year_input_dir)
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
        else:
            logging.info(f"No files found for {time_chunk}. Exiting.")
            exit(1)


if __name__ == "__main__":
    import argparse

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

    logging.info("Download Script Complete.")
