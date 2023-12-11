"""Create a mask of ocean and lake / inland water grid cells. These grid cells will be excluded from the snow metric computations. Each snow year will have an independent mask set to account for dynamic coastal and fluvial geomorphology."""

import argparse
import logging
import pickle
from pathlib import Path

import xarray as xr
import rasterio as rio
import rioxarray
import numpy as np
import dask

from config import SNOW_YEAR, preprocessed_dir

from luts import (
    n_obs_to_classify_ocean,
    n_obs_to_classify_inland_water,
    cgf_snow_cover_codes,
)

# CP note: inverting so we can reference array values by the descriptive string
inv_cgf_codes = {v: k for k, v in cgf_snow_cover_codes.items()}


def open_preprocessed_dataset(tile):
    fp = f"snow_year_{SNOW_YEAR}_{tile}.nc"
    with xr.open_dataset(preprocessed_dir / fp).CGF_NDSI_Snow_Cover.chunk(
        {"time": 52}
    ) as ds_chunked:
        return ds_chunked


def generate_ocean_mask(ds_chunked):
    """Create a mask of ocean grid cells. Locations where the number of ocean observations exceeds the threshold in a given snow year are classified as ocean for that entire snow year. Such grid cells will be excluded (masked) from the snow metric computation."""
    ocean_mask = (
        ds_chunked.where(ds == inv_cgf_codes["Ocean"]).count(dim="time")
        <= n_obs_to_classify_ocean
    )
    return ocean_mask  # may need .compute() here or later


def generate_inland_water_mask(ds_chunked):
    """Create a mask of lake / inland water grid cells. Locations where the number of lake / inland water observations exceeds the threshold in a given snow year are classified as lake / inland water for that entire snow year. Such grid cells will be excluded (masked) from the snow metric computation."""
    inland_water_mask = (
        ds_chunked.where(ds == inv_cgf_codes["Lake / Inland water"]).count(dim="time")
        <= n_obs_to_classify_inland_water
    )
    return inland_water_mask  # may need .compute() here or later


def combine_masks(ocean_mask, inland_water_mask):
    all_water_mask = np.logical_and(ocean_mask, inland_water_mask)
    return all_water_mask


def write_mask_to_geotiff():
    # probably call this thrice, one per mask
    pass


if __name__ == "__main__":
    logging.basicConfig(filename="water_masking.log", level=logging.INFO)

    parser = argparse.ArgumentParser(description="Script to Generate Water Masks")
    parser.add_argument("tile_id", type=str, help="MODIS/VIIRS Tile ID (ex. h11v02)")
    args = parser.parse_args()
    tile_id = args.tile_id

    logging.info(f"Creating water mask for tile {tile_id} for snow_year {SNOW_YEAR}.")
    print("Water Masking Script Complete.")
