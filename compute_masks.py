"""Create a mask of grid cells that will be excluded from snow metric computations. This includes ocean and lake / inland water grid cells. These grid cells will be excluded from the snow metric computations. Each snow year will have an independent mask set to account for dynamic coastal and fluvial geomorphology."""

import argparse
import logging

import xarray as xr
import numpy as np
import dask

from config import SNOW_YEAR, preprocessed_dir, mask_dir
from luts import (
    n_obs_to_classify_ocean,
    n_obs_to_classify_inland_water,
    cgf_snow_cover_codes,
)
from shared_utils import (
    open_preprocessed_dataset,
    fetch_raster_profile,
    write_tagged_geotiff,
)

# CP note: inverting to reference array values by the descriptive string
inv_cgf_codes = {v: k for k, v in cgf_snow_cover_codes.items()}


def generate_ocean_mask(ds_chunked):
    """Create a mask of ocean grid cells.

    Locations where the number of ocean observations exceeds the threshold in a given snow year are classified as ocean for that entire snow year. Such grid cells will be excluded (masked) from the snow metric computation.

    Generate a mask of ocean grid cells.

    Args:
        ds_chunked (xarray.Dataset): The chunked dataset.

    Returns:
        xarray.DataArray: The ocean mask.
    """
    logging.info(
        f"Computing ocean mask of grid cells where count of ocean observations exceeds {n_obs_to_classify_ocean} for the snow year."
    )
    ocean_mask = (
        ds_chunked.where(ds == inv_cgf_codes["Ocean"]).count(dim="time")
        <= n_obs_to_classify_ocean
    )
    return ocean_mask


def generate_inland_water_mask(ds_chunked):
    """Create a mask of lake / inland water grid cells.

    Locations where the number of lake / inland water observations exceeds the threshold in a given snow year are classified as lake / inland water for that entire snow year. Such grid cells will be excluded (masked) from the snow metric computation.

    Args:
        ds_chunked (xarray.Dataset): The chunked dataset.

    Returns:
        xarray.DataArray: The lake / inland water mask.
    """

    logging.info(
        f"Computing lake / inland water mask of grid cells where count of lake / inland water observations exceeds {n_obs_to_classify_inland_water} for the snow year."
    )
    inland_water_mask = (
        ds_chunked.where(ds == inv_cgf_codes["Lake / Inland water"]).count(dim="time")
        <= n_obs_to_classify_inland_water
    )
    return inland_water_mask


def combine_masks(ocean_mask, inland_water_mask):
    """Combine the ocean and lake / inland water masks.

    Args:
        ocean_mask (xarray.DataArray): The ocean mask.
        inland_water_mask (xarray.DataArray): The inland water mask.

    Returns:
        xarray.DataArray: The combined mask for all water grid cells.
    """
    logging.info("Combining the ocean and lake / inland water masks...")
    all_water_mask = np.logical_and(ocean_mask, inland_water_mask)
    return all_water_mask


if __name__ == "__main__":
    logging.basicConfig(filename="mask.log", level=logging.INFO)

    parser = argparse.ArgumentParser(description="Script to Generate Masks")
    parser.add_argument("tile_id", type=str, help="MODIS/VIIRS Tile ID (ex. h11v02)")
    args = parser.parse_args()
    tile_id = args.tile_id
    logging.info(f"Creating masks for tile {tile_id} for snow year {SNOW_YEAR}.")

    fp = preprocessed_dir / f"snow_year_{SNOW_YEAR}_{tile_id}.nc"
    ds = open_preprocessed_dataset(fp, {"time": "auto"}, "CGF_NDSI_Snow_Cover")

    ocean_mask = generate_ocean_mask(ds)
    inland_water_mask = generate_inland_water_mask(ds)
    all_water_mask = combine_masks(ocean_mask, inland_water_mask)

    mask_profile = fetch_raster_profile(tile_id, {"dtype": "int8", "nodata": 0})
    write_tagged_geotiff(
        mask_dir, tile_id, "mask", "ocean", mask_profile, ocean_mask.values
    )
    write_tagged_geotiff(
        mask_dir,
        tile_id,
        "mask",
        "inland_water",
        mask_profile,
        inland_water_mask.values,
    )
    write_tagged_geotiff(
        mask_dir, tile_id, "mask", "all_water", mask_profile, all_water_mask.values
    )

    print("Masking Script Complete.")
