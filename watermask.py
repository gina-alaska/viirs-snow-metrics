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

from config import SNOW_YEAR, preprocessed_dir, mask_dir
from luts import (
    n_obs_to_classify_ocean,
    n_obs_to_classify_inland_water,
    cgf_snow_cover_codes,
)
from shared_utils import open_preprocessed_dataset

# CP note: inverting to reference array values by the descriptive string
inv_cgf_codes = {v: k for k, v in cgf_snow_cover_codes.items()}


# def open_preprocessed_dataset(tile):
#     """Open a preprocessed dataset for a given tile.

#     Args:
#         tile (str): The tile identifier.

#     Returns:
#        xarray.Dataset: The chunked dataset.
#     """
#     fp = f"snow_year_{SNOW_YEAR}_{tile}.nc"
#     logging.info(f"Opening preprocessed file {fp} as chunked Dataset...")
#     # CP note: I don't think chunk values are too sensitive here, so I chose 52 for 52 weeks in a year
#     with xr.open_dataset(preprocessed_dir / fp).CGF_NDSI_Snow_Cover.chunk(
#         {"time": 52}
#     ) as ds_chunked:
#         return ds_chunked


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


def fetch_raster_profile(tile_id):
    """Fetch a raster profile to generate output mask rasters that match the downloaded NSIDC rasters.

    We load the GeoTIFF hash table to quicly extract a reference raster creation profile. Preserving these profiles should make the final alignment /
    mosaicking of the raster products a smoother process. We can also use this hash table to perform intermittent QC checks. For example, say FSD = 100 for some grid cell. We should then be able to map that value (100) to a date, then check the GeoTIFFs for that date, the date prior, and the date after, and observe the expected behavior (snow condition toggling from off to on).

    Args:
        tile_id (str): The tile identifier.

    Returns:
        dict: The raster profile.
    """

    with open("file_dict.pickle", "rb") as handle:
        geotiff_dict = pickle.load(handle)
    geotiff_reference = geotiff_dict[tile_id]["CGF_NDSI_Snow_Cover"][0]
    with rio.open(geotiff_reference) as src:
        out_profile = src.profile
    out_profile.update({"dtype": "int8"})
    out_profile.update({"nodata": 0})
    logging.info(f"Mask GeoTIFFs will use the raster creation profile {out_profile}.")
    return out_profile


def write_mask_to_geotiff(tile_id, mask_name, out_profile, arr):
    """Write a mask to a GeoTIFF file.

    Args:
        tile_id (str): The tile identifier.
        mask_name (str): The name of the mask.
        out_profile (dict): The raster profile.
        arr (numpy.ndarray): The mask array.
    """
    out_fp = mask_dir / f"{tile_id}_{mask_name}.tif"
    logging.info(f"Writing {mask_name} mask GeoTIFF to {out_fp}.")
    with rio.open(out_fp, "w", **out_profile) as dst:
        dst.update_tags(mask=mask_name)
        dst.write(arr, 1)


if __name__ == "__main__":
    logging.basicConfig(filename="mask.log", level=logging.INFO)

    parser = argparse.ArgumentParser(description="Script to Generate Masks")
    parser.add_argument("tile_id", type=str, help="MODIS/VIIRS Tile ID (ex. h11v02)")
    args = parser.parse_args()
    tile_id = args.tile_id
    logging.info(f"Creating masks for tile {tile_id} for snow year {SNOW_YEAR}.")

    #ds = open_preprocessed_dataset(tile_id)
    fp = preprocessed_dir / f"snow_year_{SNOW_YEAR}_{tile_id}.nc"
    ds = open_preprocessed_dataset(fp, {"time": "auto"}, "CGF_NDSI_Snow_Cover")
    ocean_mask = generate_ocean_mask(ds)
    inland_water_mask = generate_inland_water_mask(ds)
    all_water_mask = combine_masks(ocean_mask, inland_water_mask)
    out_profile = fetch_raster_profile(tile_id)

    write_mask_to_geotiff(tile_id, "ocean", out_profile, ocean_mask.values)
    write_mask_to_geotiff(
        tile_id, "inland_water", out_profile, inland_water_mask.values
    )
    write_mask_to_geotiff(tile_id, "all_water", out_profile, all_water_mask.values)

    print("Masking Script Complete.")
