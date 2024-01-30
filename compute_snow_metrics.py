"""Compute the VIIRS snow metrics."""

import logging
import argparse
import calendar

import xarray as xr
import rasterio as rio
import rioxarray
import dask
import numpy as np

from config import preprocessed_dir, mask_dir, single_metric_dir, SNOW_YEAR
from luts import snow_cover_threshold, cgf_snow_cover_codes
from shared_utils import (
    open_preprocessed_dataset,
    fetch_raster_profile,
    write_tagged_geotiff,
)


def apply_threshold(chunked_cgf_snow_cover):
    """Apply the snow cover threshold to the CGF snow cover datacube. Grid cells exceeding the threshold value are considered to be snow-covered.

    Note that 100 is the maximum valid snow cover value.
    
    Args:
        chunked_cgf_snow_cover (xr.DataArray): preprocessed CGF snow cover datacube
    Returns:
        snow_on (xr.DataArray): boolean values representing snow cover"""
    snow_on = ((chunked_cgf_snow_cover > snow_cover_threshold) & (chunked_cgf_snow_cover <= 100))
    return snow_on


def fill_winter_darkness(chunked_cgf_snow_cover):
    """
    Fill winter darkness with the snow cover value from the previous day.
    """
    chunked_cgf_snow_cover = chunked_cgf_snow_cover.where(chunked_cgf_snow_cover != cgf_snow_cover_codes["Night"], np.nan)
    chunked_cgf_snow_cover = chunked_cgf_snow_cover.ffill(dim="time")
    return chunked_cgf_snow_cover


def shift_to_day_of_snow_year_values(doy_arr):
    """Day-of-snow-year corresponds to the familiar day of year (i.e., 1 to 365), but spans two calendar years (e.g., the first day of the second year is day 1 + 365 = 366). Because our snow year is defined as August 1 to July 31, possible values for day-of-snow-year are 213 to 577 (1 August is day of year 213; 31 July is day of year 212 + 365 = 577). When $SNOW_YEAR + 1 is a leap year, the maximum value may be 578."""

    leap_year = calendar.isleap(int(SNOW_YEAR) + 1)
    if not leap_year:
        doy_arr += 212
    else:
        doy_arr += 213
    return doy_arr


def get_first_snow_day_array(snow_on):
    """Compute first snow day (FSD) of the full snow season (FSS start day).

    Args:
       snow_on (xr.DataArray): boolean values representing snow cover

    Returns:
        xr.DataArray: integer values representing the day of year value where the CGF snowcover exceeds a threshold value for the first time.
    """
    fsd_array = snow_on.argmax(dim="time")
    fsd_array += 1  # bump value by one, because argmax yields an index, and we index from 0, but don't want 0 values to represent a DOY in the output
    return shift_to_day_of_snow_year_values(fsd_array)


def get_last_snow_day_array(snow_on):
    """Compute last snow day (LSD) of the full snow season (FSS end day).

    The logic for last snow day is the same as for first snow day - but the time dimension of the input DataArray is reversed.

    Args:
       snow_on (xr.DataArray): boolean values representing snow cover

    Returns:
        xr.DataArray: integer values representing the day of year value where the CGF snowcover exceeds a threshold value for the final time.
    """

    snow_on_reverse_time = snow_on.isel(time=slice(None, None, -1))
    last_occurrence_reverse = snow_on_reverse_time.argmax(dim="time")
    # must revert time indices back to the original order
    lsd_array = snow_on.time.size - last_occurrence_reverse - 1
    # we could could omit the `- 1` above, but we'll be explicit and match the value bump used in FSD
    lsd_array += 1
    return shift_to_day_of_snow_year_values(lsd_array)


def compute_full_snow_season_range(lsd_array, fsd_array):
    """Compute range (i.e., length) of the full snow season."""
    return lsd_array - fsd_array - 1


def apply_mask(mask_fp, array_to_mask):
    """Mask out values from the snow metric array."""
    with rio.open(mask_fp) as src:
        mask_arr = src.read(1)
    mask_applied = mask_arr * array_to_mask
    return mask_applied


if __name__ == "__main__":
    logging.basicConfig(filename="compute_metrics.log", level=logging.INFO)

    parser = argparse.ArgumentParser(description="Snow Metric Computation Script")
    parser.add_argument("tile_id", type=str, help="MODIS/VIIRS Tile ID (ex. h11v02)")
    args = parser.parse_args()
    tile_id = args.tile_id
    logging.info(f"Computing snow metrics for tile {tile_id}.")

    fp = preprocessed_dir / f"snow_year_{SNOW_YEAR}_{tile_id}.nc"

    chunky_ds = open_preprocessed_dataset(
        fp, {"x": "auto", "y": "auto"}, "CGF_NDSI_Snow_Cover"
    )
 
    snow_metrics = dict()
    snow_metrics.update({"first_snow_day": get_first_snow_day_array(chunky_ds)})
    snow_metrics.update({"last_snow_day": get_last_snow_day_array(chunky_ds)})
    snow_metrics.update(
        {
            "fss_range": compute_full_snow_season_range(
                snow_metrics["last_snow_day"], snow_metrics["first_snow_day"]
            )
        }
    )

    single_metric_profile = fetch_raster_profile(
        tile_id, {"dtype": "int16", "nodata": 0}
    )
    for metric_name, metric_array in snow_metrics.items():
        write_tagged_geotiff(
            single_metric_dir,
            tile_id,
            "snow_metric",
            metric_name,
            single_metric_profile,
            metric_array.compute().values.astype("int16")
            # we don't actually have to call .compute(), but this communicates a chunked DataArray input and there is no performance penalty vs. just calling .values
        )

    print("Snow Metric Computation Script Complete.")
