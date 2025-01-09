"""Create a mask of grid cells that will be excluded from snow metric computations. This includes ocean and lake / inland water grid cells. These grid cells will be excluded from the snow metric computations. Each snow year will have an independent mask set to account for dynamic coastal and fluvial geomorphology."""

import argparse
import logging
import os

import numpy as np
from dask.distributed import Client

from config import SNOW_YEAR, preprocessed_dir, mask_dir
from luts import n_obs_to_classify_ocean, n_obs_to_classify_inland_water, inv_cgf_codes
from shared_utils import (
    open_preprocessed_dataset,
    fetch_raster_profile,
    write_tagged_geotiff,
)


def generate_l2fill_mask(ds_chunked):
    """Create a mask of grid cells with a constant time series of L2 fill no data values.

    Args:
        ds_chunked (xarray.Dataset): The chunked dataset.
    Returns:
        xarray.DataArray: The no data mask.
    """
    logging.info(
        "Computing no data mask of grid cells with a constant time series of L2 fill no data values."
    )
    l2fill_mask = (ds_chunked != inv_cgf_codes["L2 fill"]).all(dim="time")
    return l2fill_mask


def generate_ocean_mask(ds_chunked):
    """Create a mask of ocean grid cells.

    Locations where the number of ocean observations exceeds the threshold in a given snow year are classified as ocean for that entire snow year. Such grid cells will be excluded (masked) from the snow metric computation.

    Args:
        ds_chunked (xarray.Dataset): The chunked dataset.

    Returns:
        xarray.DataArray: The ocean mask.
    """
    logging.info(
        f"Computing ocean mask of grid cells where count of ocean observations exceeds {n_obs_to_classify_ocean} for the snow year."
    )
    ocean_mask = (
        ds_chunked.where(ds_chunked == inv_cgf_codes["Ocean"]).count(dim="time")
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
        ds_chunked.where(ds_chunked == inv_cgf_codes["Lake / Inland water"]).count(
            dim="time"
        )
        <= n_obs_to_classify_inland_water
    )
    return inland_water_mask


def combine_masks(mask_list):
    """Combine individual mask layers.

    Where an individual mask is True, it indicates data is valid with respect to that particular mask condition. When False, it indicates the element should be masked out for that particular condition. Individual masks are combined into a single mask array such that if an element has a False value for any of the input masks, it is False in the output. An element must be True for all of the input mask arrays for it to be True in the output.

    Args:
        mask_list (list of xr.DataArray objects): list of masks (e.g., ocean, inland water, etc.)

    Returns:
        xarray.DataArray: The combined mask of grid cells.
    """
    logging.info("Combining masks...")
    masks_combined = np.all(mask_list, axis=0)
    return masks_combined


if __name__ == "__main__":
    log_file_path = os.path.join(os.path.expanduser("~"), "mask_computation.log")
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', filename=log_file_path, level=logging.INFO)

    parser = argparse.ArgumentParser(description="Script to Generate Masks")
    parser.add_argument("tile_id", type=str, help="VIIRS Tile ID (ex. h11v02)")
    args = parser.parse_args()
    tile_id = args.tile_id
    logging.info(f"Creating masks for tile {tile_id} for snow year {SNOW_YEAR}.")
    client = Client(n_workers=24)
    fp = preprocessed_dir / f"snow_year_{SNOW_YEAR}_{tile_id}.nc"
    ds = open_preprocessed_dataset(
        fp, {"x": "auto", "y": "auto"}, "CGF_NDSI_Snow_Cover"
    )

    ocean_mask = generate_ocean_mask(ds)
    inland_water_mask = generate_inland_water_mask(ds)
    l2_mask = generate_l2fill_mask(ds)
    combined_mask = combine_masks([ocean_mask, inland_water_mask, l2_mask])

    mask_profile = fetch_raster_profile(tile_id, {"dtype": "int8", "nodata": 0})
    write_tagged_geotiff(
        mask_dir, tile_id, "_mask", "ocean", mask_profile, ocean_mask.values
    )
    write_tagged_geotiff(
        mask_dir,
        tile_id,
        "_mask",
        "inland_water",
        mask_profile,
        inland_water_mask.values,
    )
    write_tagged_geotiff(
        mask_dir,
        tile_id,
        "_mask",
        "l2_fill",
        mask_profile,
        l2_mask.values,
    )
    write_tagged_geotiff(
        mask_dir, tile_id, "_mask", "combined", mask_profile, combined_mask
    )
    ds.close()
    client.close()
    print("Mask Generation Script Complete.")
