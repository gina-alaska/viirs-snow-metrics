"""Utility functions used across multiple modules."""

import logging
import pickle
from pathlib import Path

import xarray as xr
import rasterio as rio

from config import SNOW_YEAR


def open_preprocessed_dataset(fp, chunk_dict, data_variable):
    """Open a preprocessed dataset for a given tile.

    Args:
        fp (Path): Path to xarray DataSet
        chunk_dict (dict): how to chunk the dataset, like `{"time": 52}`

    Returns:
       xarray.Dataset: The chunked dataset.
    """
    logging.info(f"Opening preprocessed file {fp} as chunked Dataset...")

    with xr.open_dataset(fp)[data_variable].chunk(chunk_dict) as ds_chunked:
        return ds_chunked


def fetch_raster_profile(tile_id, updates=None):
    """Fetch a raster profile to generate output mask rasters that match the downloaded NSIDC rasters.

    We load the GeoTIFF hash table to quicly extract a reference raster creation profile. Preserving these profiles should make the final alignment /
    mosaicking of the raster products a smoother process. We can also use this hash table to perform intermittent QC checks. For example, say FSD = 100 for some grid cell. We should then be able to map that value (100) to a date, then check the GeoTIFFs for that date, the date prior, and the date after, and observe the expected behavior (snow condition toggling from off to on).

    Args:
        tile_id (str): The tile identifier.
        updates (dict): Modifications to the intial raster creation profile e.g., `{"dtype": "int8", "nodata": 0}`
    Returns:
        dict: The raster profile.
    """

    with open("file_dict.pickle", "rb") as handle:
        geotiff_dict = pickle.load(handle)
    geotiff_reference = geotiff_dict[tile_id]["CGF_NDSI_Snow_Cover"][0]
    with rio.open(geotiff_reference) as src:
        out_profile = src.profile
    if updates is not None:
        out_profile.update(updates)
    logging.info(f"GeoTIFFs will use the raster creation profile {out_profile}.")
    return out_profile


def write_tagged_geotiff(dst_dir, tile_id, tag_name, tag_value, out_profile, arr):
    """Write data to a GeoTIFF file.

    Not for multiband or multi-tile GeoTIFFs. Use for masks, single metrics, and other intermediate data products.

    Args:
        dst_dir (Path): Output directory for the GeoTIFF
        tile_id (str): The tile identifier.
        tag_name (str): The name of the metadata tag.
        tag_value (str): Value of the metadata tag.
        out_profile (dict): The raster profile.
        arr (numpy.ndarray): The mask array.

    Returns:
        None
    """
    out_fp = dst_dir / f"{tile_id}_{tag_name}_{tag_value}_{SNOW_YEAR}.tif"
    logging.info(f"Writing GeoTIFF to {out_fp}.")
    with rio.open(out_fp, "w", **out_profile) as dst:
        dst.update_tags(tag_name=tag_value)
        dst.write(arr, 1)
    return None
