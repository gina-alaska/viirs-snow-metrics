"""Gather data to characterize uncertainty within each tile including cloud persistence, cloud coverage, no decision, etc."""

import argparse
import logging

import numpy as np
from dask.distributed import Client

from config import SNOW_YEAR, preprocessed_dir, uncertainty_dir
from luts import inv_cgf_codes
from shared_utils import (
    open_preprocessed_dataset,
    fetch_raster_profile,
    write_tagged_geotiff,
)


def count_no_decision_occurence(ds_chunked):
    """Count the per-pixel occurrence of "no decision" in the snow year.

    Args:
        ds_chunked (xarray.Dataset): The chunked dataset.

    Returns:
        xarray.DataArray: count of "No decision" values".
    """

    logging.info(f"Counting occurence of `No decision` values...")
    no_decision_count = (ds_chunked == inv_cgf_codes["No decision"]).sum(dim="time")
    return no_decision_count


def count_missing_l1b_occurence(ds_chunked):
    """Count the per-pixel occurrence of "Missing L1B data" in the snow year.

    Args:
        ds_chunked (xarray.Dataset): The chunked dataset.

    Returns:
        xarray.DataArray: count of "Missing L1B data" values".
    """

    logging.info(f"Counting occurence of `Missing L1B data` values...")
    missing_l1b_count = (ds_chunked == inv_cgf_codes["Missing L1B data"]).sum(
        dim="time"
    )
    return missing_l1b_count


def count_l1b_calibration_fail(ds_chunked):
    """Count the per-pixel occurrence of "L1B data failed calibration" in the snow year.

    Args:
        ds_chunked (xarray.Dataset): The chunked dataset.

    Returns:
        xarray.DataArray: count of "L1B data failed calibration" values".
    """

    logging.info(f"Counting occurence of `L1B data failed calibration` values...")
    l1b_fail_count = (ds_chunked == inv_cgf_codes["L1B data failed calibration"]).sum(
        dim="time"
    )
    return l1b_fail_count


def count_bowtie_trim(ds_chunked):
    """Count the per-pixel occurrence of "Onboard VIIRS bowtie trim" in the snow year.

    Args:
        ds_chunked (xarray.Dataset): The chunked dataset.

    Returns:
        xarray.DataArray: count of "Onboard VIIRS bowtie trim" values".
    """

    logging.info(f"Counting occurence of `Onboard VIIRS bowtie trim` values...")
    bowtie_trim_count = (ds_chunked == inv_cgf_codes["Onboard VIIRS bowtie trim"]).sum(
        dim="time"
    )
    return bowtie_trim_count


def count_darkness(ds_chunked):
    """Count the per-pixel occurrence of polar/winter darkness in the snow year.

    Args:
        ds_chunked (xarray.Dataset): The chunked dataset.

    Returns:
        xarray.DataArray: count of "Night" values".
    """

    logging.info(f"Counting occurence of `Night` values...")
    darkness_count = (ds_chunked == inv_cgf_codes["Night"]).sum(dim="time")
    return darkness_count


def get_max_cloud_persistence(ds_chunked):
    """Determine maximum per-pixel cloud persistence value.

    Args:
        ds_chunked (xarray.Dataset): The chunked dataset.

    Returns:
        xarray.DataArray: max cloud persistence value".
    """

    logging.info(f"Finding maximum cloud persistence value...")
    max_cloud_persist = ds_chunked.max(dim="time")
    return max_cloud_persist


if __name__ == "__main__":
    logging.basicConfig(filename="gather_uncertainty.log", level=logging.INFO)
    parser = argparse.ArgumentParser(
        description="Script to Fetch Data For Uncertainty Analysis"
    )
    parser.add_argument("tile_id", type=str, help="VIIRS Tile ID (ex. h11v02)")
    args = parser.parse_args()
    tile_id = args.tile_id
    logging.info(
        f"Gathering uncertainty data for tile {tile_id} for snow year {SNOW_YEAR}."
    )
    client = Client()
    uncertainty_data = dict()
    fp = preprocessed_dir / f"snow_year_{SNOW_YEAR}_{tile_id}.nc"

    cgf_snow_ds = open_preprocessed_dataset(
        fp, {"x": "auto", "y": "auto"}, "CGF_NDSI_Snow_Cover"
    )
    uncertainty_data.update({"no decision": count_no_decision_occurence(cgf_snow_ds)})
    uncertainty_data.update({"missing L1B": count_missing_l1b_occurence(cgf_snow_ds)})
    uncertainty_data.update({"L1B fail": count_l1b_calibration_fail(cgf_snow_ds)})
    uncertainty_data.update({"bowtie trim": count_bowtie_trim(cgf_snow_ds)})
    uncertainty_data.update({"darkness": count_darkness(cgf_snow_ds)})
    cgf_snow_ds.close()

    cloud_ds = open_preprocessed_dataset(
        fp, {"x": "auto", "y": "auto"}, "Cloud_Persistence"
    )
    uncertainty_data.update(
        {"max cloud persistence": get_max_cloud_persistence(cgf_snow_ds)}
    )
    cloud_ds.close()

    out_profile = fetch_raster_profile(tile_id, {"dtype": "int16", "nodata": 0})

    for uncertainty_name, uncertainty_array in uncertainty_data.items():
        if uncertainty_array.sum().compute() == 0:
            logging.info(
                f"No occurences found for {uncertainty_name}. A GeoTIFF will not be written."
            )
            continue
        write_tagged_geotiff(
            uncertainty_dir,
            tile_id,
            "",
            uncertainty_name,
            out_profile,
            uncertainty_array.compute().values.astype("int16"),
        )

    client.close()
    print("Uncertainty Data Fetch Complete.")
