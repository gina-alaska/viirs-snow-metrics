"""Compute the VIIRS snow metrics."""
import pickle
from datetime import datetime, timedelta
from pathlib import Path

import xarray as xr
import rasterio as rio
import rioxarray
import dask
import numpy as np

from config import preprocessed_dir
from luts import snow_cover_threshold


def get_first_snow_day_array(chunked_ds):
    """Compute first day of the full snow season (FSS start day)."""
    # actual logic has to be (snow threshold < ds value <= 100)
    fsd_array = (chunked_ds["CGF_NDSI_Snow_Cover"] > snow_cover_threshold).argmax(
        dim="time"
    )
    # may need to bump this value by one, because argmax yields an index, and we index from zero
    return fsd_array.values.astype("int16")  # must verify dtype
    # may want to just the delayed chunk, and only go for .values / .compute when writing the GeoTIFF


def get_last_snow_day_array(chunked_ds):
    """Compute last day of the full snow season (FSS start day)."""
    ## reverse this
    lsd_array = (chunked_ds["CGF_NDSI_Snow_Cover"] > snow_cover_threshold).argmax(
        dim="time"
    )
    # may need to bump this value by one, because argmax yields an index, and we index from zero
    return lsd_array.values.astype("int16")  # must verify dtype
    # may want to just the delayed chunk, and only go for .values / .compute when writing the GeoTIFF


def shift_to_day_of_snow_year_values(doy_arr):
    """Day-of-snow-year corresponds to the familiar day of year (i.e., 1 to 365), but spans two calendar years (e.g., the first day of the second year is day 1 + 365 = 366). Because our snow year is defined as August 1 to July 31, possible values for day-of-snow-year are 213 to 577 (1 August is day of year 213; 31 July is day of year 212 + 365 = 577). When $SNOW_YEAR + 1 is a leap year, the maximum value may be 578."""
    # leap_year = is_leap_year()
    leap_year = None  # placeholder
    # this assumes that the "earliest" value is 1
    if not leap:
        dosy_arr = doy_arr + 212
    else:
        dosy_arr = doy_arr + 213
    return dosy_arr
    pass


def compute_full_snow_season_range(fsd_array, lsd_array):
    """Compute range (i.e., length) of the full snow season."""
    return (lsd_array - fsd_array) - 1


def map_nodata_values():
    pass
