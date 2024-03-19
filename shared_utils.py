"""Utility functions used across multiple modules."""

import logging
import pickle

import xarray as xr
import rasterio as rio

from luts import snow_cover_threshold
from config import SNOW_YEAR, preprocessed_dir


def open_preprocessed_dataset(fp, chunk_dict, data_variable=None):
    """Open a preprocessed dataset for a given tile.

    Args:
        fp (Path): Path to xarray DataSet
        chunk_dict (dict): how to chunk the dataset, like `{"time": 52}`

    Returns:
       xr.Dataset: The chunked dataset.
    """
    logging.info(f"Opening preprocessed file {fp} as chunked Dataset...")
    if data_variable is not None:
        with xr.open_dataset(fp)[data_variable].chunk(chunk_dict) as ds_chunked:
            return ds_chunked
    else:
        with xr.open_dataset(fp).chunk(chunk_dict) as ds_chunked:
            return ds_chunked


def write_single_tile_xrdataset(ds, tile, suffix=None):
    """Write the DataSet to a netCDF file.

    Args:
       ds (xr.Dataset): The single-tile dataset.
       tile (str): The tile being processed.
       suffix (str): An optional suffix to append to the filename.
    """
    if suffix is not None:
        filename = preprocessed_dir / f"snow_year_{SNOW_YEAR}_{tile}_{suffix}.nc"
    else:
        filename = preprocessed_dir / f"snow_year_{SNOW_YEAR}_{tile}.nc"
    ds.to_netcdf(filename)
    logging.info(f"NetCDF dataset for tile {tile} wriiten to {filename}.")


def apply_threshold(chunked_cgf_snow_cover):
    """Apply the snow cover threshold to the CGF snow cover datacube. Grid cells exceeding the threshold value are considered to be snow-covered.

    Note that 100 is the maximum valid snow cover value.

    Args:
        chunked_cgf_snow_cover (xr.DataArray): preprocessed CGF snow cover datacube

    Returns:
        snow_on (xr.DataArray): boolean values representing snow cover"""
    snow_on = (chunked_cgf_snow_cover > snow_cover_threshold) & (
        chunked_cgf_snow_cover <= 100
    )
    return snow_on


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


def apply_mask(mask_fp, array_to_mask):
    """Mask out values from an array.

    Args:
        mask_fp (str): file path to the mask GeoTIFF
        array_to_mask (xr.DataArray): array to be masked
    Returns:
        xr.DataArray: masked array where masked values are set to 0
    """

    with rio.open(mask_fp) as src:
        mask_arr = src.read(1)
    mask_applied = mask_arr * array_to_mask
    return mask_applied


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
