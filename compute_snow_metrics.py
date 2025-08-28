"""Compute the VIIRS snow metrics."""

import logging
import argparse
import calendar
import os

import rasterio as rio
import numpy as np
import xarray as xr
from dask.distributed import Client

from config import (
    preprocessed_dir,
    mask_dir,
    single_metric_dir,
    SNOW_YEAR,
)
from luts import (
    snow_cover_threshold,
    inv_cgf_codes,
    css_days_threshold,
)
from shared_utils import (
    open_preprocessed_dataset,
    fetch_raster_profile,
    apply_threshold,
    apply_mask,
    write_tagged_geotiff,
    write_tagged_geotiff_from_data_array,
)


def shift_to_day_of_snow_year_values(doy_arr, needs_leap_shift=False):
    """Shifts day-of-year values to day-of-snow-year values.

    Day-of-snow-year values mimic familiar day-of-year values (i.e., 1 to 365) but span two calendar years (e.g., the first day of the second calendar year = 1 + 365 = 366). A snow year is defined as August 1 through July 31, and so possible values for day-of-snow-year are 213 to 577 (1 August is day of year 213; 31 July is day of year (212 + 365) = 577. When $SNOW_YEAR + 1 is a leap year, the maximum value may be 578.

    Args:
        doy_arr (array-like): Day-of-year values.
        needs_leap_shift (bool): whether to shift the values by one day for leap years - even if the snow year contains a leap year, metrics where the leap day has not been accrued should not be shifted (e.g., FSD). Feb. 29th is the 60th day of a leap year.

    Returns:
        array-like: Day-of-snow-year values.
    """

    leap_year = calendar.isleap(int(SNOW_YEAR) + 1)
    if leap_year and needs_leap_shift:
        doy_arr += 213
    else:
        doy_arr += 212
    return doy_arr


def get_first_snow_day_array(snow_on):
    """Compute first snow day (FSD) of the full snow season (FSS start day).

    Args:
       snow_on (xr.DataArray): boolean values representing snow cover

    Returns:
        xr.DataArray: integer values representing the day-of-snow-year value where the CGF snowcover exceeds a threshold value for the first time.
    """
    fsd_array = snow_on.argmax(dim="time")
    fsd_array += 1  # bump value by one, because argmax yields an index, and we index from 0, but don't want 0 values to represent a DOY in the output
    return shift_to_day_of_snow_year_values(fsd_array)


def get_last_snow_day_array(snow_on):
    """Compute last snow day (LSD) of the full snow season (FSS end day).

    The logic for last snow day is the same as for first snow day, but the time dimension of the input DataArray is reversed.

    Args:
       snow_on (xr.DataArray): boolean values representing snow cover

    Returns:
        xr.DataArray: integer values representing the day-of-snow-year value where the CGF snowcover exceeds a threshold value for the final time.
    """

    snow_on_reverse_time = snow_on.isel(time=slice(None, None, -1))
    last_occurrence_reverse = snow_on_reverse_time.argmax(dim="time")
    # must revert time indices back to the original order
    lsd_array = snow_on.time.size - last_occurrence_reverse - 1
    # we could just could omit the `- 1` above...
    # but we'll be explicit and match index-to-DOY pattern used in FSD
    # lsd_array += 1
    return shift_to_day_of_snow_year_values(lsd_array, needs_leap_shift=True)


def count_snow_days(snow_on):
    """Count the number of snow-covered days in a snow season.

    Args:
        snow_on (xr.DataArray): boolean values representing snow cover

    Returns:
        xr.DataArray: integer values representing the number of snow days in the snow season.
    """
    return snow_on.sum(dim="time")


def count_no_snow_days(cgf_snow_darkness_filled):
    """Count the number of snow-free days in a snow season.

    Args:
        cgf_snow_darkness_filled (xr.DataArray): darkness-filled cloud-gap filled snow cover

    Returns:
        xr.DataArray: integer values representing the number of no-snow days in the snow season.
    """
    snow_off_days = cgf_snow_darkness_filled <= snow_cover_threshold
    return snow_off_days.sum(dim="time")


def compute_full_snow_season_range(lsd_array, fsd_array):
    """Compute range (i.e., length) of the full snow season.

    Args:
        lsd_array (xr.DataArray): last snow day (LSD) values.
        fsd_array (xr.DataArray): first snow day (FSD) values.

    Returns:
    xr.DataArray: lengths of the full snow seasons.
    """
    return lsd_array - fsd_array + 1


def _continuous_snow_season_metrics(time_series):
    """Compute metrics related to continuous snow season (CSS) from a time series of snow data.

    This function returns a tuple of five values representing five different CSS metrics: first CSS day, last CSS day, CSS range, number of discrete CSS segments, and total number of days within CSS segments. If the time series does not contain one or more CSS segments, the function returns a five-tuple of zeros.

    First and last CSS day metrics are initially have values computed from the index values of the time series. These values are incremented by 1 to convert them from index values to day-of-year values, and then these values are converted again to day-of-snow-year values. The other metrics represent either counts or durations and thus are not shifted to day-of-snow-year values. This function is intended to be used with xr.apply_ufunc.

    Args:
        time_series (xr.DataArray or numpy array): A time series of snow data, where True represents 'snow on' and False represents 'snow off'.

    Returns:
        tuple: A tuple of five CSS metrics.
    """
    # tuples for special css cases
    # when there is no css, values of 0 represents no css data
    no_css = tuple([0] * 5)
    # when snow cover is always on (e.g., glaciers)
    leap_year = calendar.isleap(int(SNOW_YEAR) + 1)
    if not leap_year:
        year_length = 365
        snow_year_doy_end = 577
    else:
        year_length = 366
        snow_year_doy_end = 578
    glacier_css = (213, snow_year_doy_end, year_length, 1, year_length)

    # xr.apply_ufunc fails on all False (never "snow on" conditions) series without this block
    if not np.any(time_series):
        return no_css
    if np.all(time_series):
        return glacier_css

    # 1 where time_series is True (i.e., snow is on), and 0 where False
    streaks = np.where(time_series, 1, 0)
    # CSS can have intervening snow-free periods of some max duration
    # tolerate up to two consecutive False values in a streak
    for i in range(1, len(streaks) - 1):
        if streaks[i] == 0 and streaks[i - 1] == 1 and streaks[i + 1] == 1:
            streaks[i] = 1
    # difference between consecutive elements in streaks
    diff = np.diff(streaks)
    # diff is 1 (the start of a streak, 0 to 1) or -1 (end of a streak, 1 to 0)
    start_indices = np.where(diff == 1)[0] + 1
    end_indices = np.where(diff == -1)[0]

    # CP note: np.r_ a convenience function for concatenating arrays, basically injecting start/end indices into the arrays when needed to handle edge cases
    # case when a streak starts on day index 0
    if streaks[0] == 1:
        start_indices = np.r_[0, start_indices]
    # case when a streak ends on the last day index
    if streaks[-1] == 1:
        end_indices = np.r_[end_indices, len(streaks) - 1]

    # find longest streak of a minimum duration
    lengths = end_indices - start_indices
    # number of css segments
    css_segment_num = np.where(lengths >= css_days_threshold)[0].size
    # total number of css days
    tot_css_days = lengths[np.where(lengths >= css_days_threshold)].sum()

    # get longest css
    longest_streak_index = np.argmax(lengths)
    # if no streak is minimum duration or longer, there are no css metrics
    if lengths[longest_streak_index] < css_days_threshold:
        return no_css

    # otherwise, return the metrics
    longest_css_start = start_indices[longest_streak_index]
    longest_css_end = end_indices[longest_streak_index]
    longest_css_range = longest_css_end - longest_css_start + 1
    # shift from time index values to DOY values
    longest_css_start += 1
    longest_css_end += 1
    return (
        shift_to_day_of_snow_year_values(longest_css_start),
        shift_to_day_of_snow_year_values(longest_css_end),
        longest_css_range,
        css_segment_num,
        tot_css_days,
    )


def compute_css_metrics(snow_on):
    """Compute metrics related to continuous snow season (CSS) from a time series of snow data.

    Args:
        snow_on (xr.DataArray): boolean values representing snow cover

    Returns:
        dict: A dictionary of CSS metrics."""
    css_results = xr.apply_ufunc(
        _continuous_snow_season_metrics,
        snow_on,
        input_core_dims=[["time"]],
        output_dtypes=[float, float, float, float, float],
        output_core_dims=[[], [], [], [], []],
        vectorize=True,
        dask="parallelized",
    )
    css_metric_dict = dict(
        zip(
            [
                "longest_css_start",
                "longest_css_end",
                "longest_css_range",
                "css_segment_num",
                "tot_css_days",
            ],
            css_results,
        )
    )

    return css_metric_dict


def process_snow_metrics(chunky_ds, combined_mask):
    snow_is_on = apply_threshold(chunky_ds)
    snow_metrics = dict()
    snow_metrics.update({"first_snow_day": get_first_snow_day_array(snow_is_on)})
    snow_metrics.update({"last_snow_day": get_last_snow_day_array(snow_is_on)})

    snow_metrics.update(
        {
            "fss_range": compute_full_snow_season_range(
                snow_metrics["last_snow_day"], snow_metrics["first_snow_day"]
            )
        }
    )
    snow_metrics.update({"snow_days": count_snow_days(snow_is_on)})
    snow_metrics.update({"no_snow_days": count_no_snow_days(chunky_ds)})
    snow_metrics.update(compute_css_metrics(snow_is_on))

    # iterate through keys in snow_metrics dict and apply mask
    for metric_name, metric_array in snow_metrics.items():
        snow_metrics[metric_name] = apply_mask(combined_mask, metric_array)
    return snow_metrics


def main(tile_id, format, alt_input=None):
    logging.info(f"Computing snow metrics for tile {tile_id}.")
    # A Dask LocalCluster speeds this script up 10X
    client = Client(n_workers=9)
    print("Monitor the Dask client dashboard for progress at the link below:")
    print(client.dashboard_link)

    kwargs = {"decode_coords": "all"} if format == "h5" else {}

    if alt_input is not None:
        logging.info(f"Using alternate input file: {alt_input}")
        fp = preprocessed_dir / f"snow_year_{SNOW_YEAR}_{tile_id}_{alt_input}.nc"
        chunky_ds = open_preprocessed_dataset(
            fp, {"x": "auto", "y": "auto"}, "CGF_NDSI_Snow_Cover", **kwargs
        )
        output_tag = alt_input
    else:
        fp = preprocessed_dir / f"snow_year_{SNOW_YEAR}_{tile_id}_filtered_filled.nc"
        chunky_ds = open_preprocessed_dataset(
            fp, {"x": "auto", "y": "auto"}, "CGF_NDSI_Snow_Cover", **kwargs
        )

    logging.info(f"Applying Snow Cover Threshold...")

    combined_mask = mask_dir / f"{tile_id}_mask_combined_{SNOW_YEAR}.tif"

    snow_metrics = process_snow_metrics(chunky_ds, combined_mask)

    if format == "h5":
        for metric_name, metric_array in snow_metrics.items():
            metric_array.name = metric_name
            metric_array.rio.set_nodata(0, inplace=True)
            write_tagged_geotiff_from_data_array(
                single_metric_dir,
                tile_id,
                "",
                metric_name,
                metric_array,
                dtype="int16",
            )
            metric_array.close()

    else:
        single_metric_profile = fetch_raster_profile(
            tile_id, {"dtype": "int16", "nodata": 0}
        )
        for metric_name, metric_array in snow_metrics.items():
            write_tagged_geotiff(
                single_metric_dir,
                tile_id,
                "",
                metric_name,
                single_metric_profile,
                metric_array.compute().values.astype("int16"),
                # don't have to call .compute(), but communicates a chunked DataArray input
            )

    client.close()
    chunky_ds.close()


if __name__ == "__main__":
    log_file_path = os.path.join(os.path.expanduser("~"), "snow_metric_computation.log")
    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(message)s",
        filename=log_file_path,
        level=logging.INFO,
    )
    parser = argparse.ArgumentParser(description="Snow Metric Computation Script")
    parser.add_argument("tile_id", type=str, help="VIIRS Tile ID (ex. h11v02)")
    parser.add_argument(
        "--format",
        "-f",
        choices=["tif", "h5"],
        default="h5",
        help="Download/input File format: Older processing methods use tif, newer uses h5",
    )
    parser.add_argument(
        "--alt_input",
        type=str,
        help="Alternate input file indicated by filename suffix.",
    )
    args = parser.parse_args()

    main(args.tile_id, args.format, args.alt_input)

    print("Snow Metric Computation Script Complete.")
