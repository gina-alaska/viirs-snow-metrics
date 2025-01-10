"Apply a filter to low illumination observations and fill in data gaps caused by night and cloud conditions."

import os
import argparse
import logging
import xarray as xr
import numpy as np
from scipy.signal import savgol_filter
from dask.distributed import Client

from config import (
    preprocessed_dir,
    SNOW_YEAR,
)
from luts import inv_cgf_codes
from shared_utils import open_preprocessed_dataset, write_single_tile_xrdataset


def is_low_illumination_for_solar_zenith(bitflag_value):
    """Determine if the bitflag value indicates a low illumination condition where solar zenith angles less than 70 degrees.

    Note that the bitflag can represent multiple conditions, e.g., 2^7 = 128 indicates low illumination, but so does 2^7 + 2^0 = 129 (low illumination condition and the 'Inland water screen' condition are both present). This function helps identify candidate values for filtering.

    Args:
        bitflag_value (int): The bitflag value.
    Returns:
        bool: Whether the bitflag value indicates low illumination due to solar zenith angles less than 70 degrees.
    """
    return (bitflag_value & 128) != 0


def is_snow_valid_and_nonzero(snowcover_value):
    """Determine if the snowcover value is valid and nonzero.

    This function helps identify candidate values for filtering. We only want to filter valid snowcover values because values outside the valid range (0-100) have other important information (e.g., cloud, night, water, etc.) that we need to retain.

    Args:
        snowcover_value (int): The snowcover value.
    Returns:
        bool: Whether or not the snowcover value is valid.
    """
    return (snowcover_value >= 1) & (snowcover_value <= 100)


def identify_sections(mask):
    """
    Identify continuous true sections in a mask array.

    Args:
        mask (np.array): 1D boolean array indicating where to apply the filter or fill.

    Returns:
        list of slices: Each slice represents a section to filter or fill.
    """
    true_indices = np.flatnonzero(mask)
    if true_indices.size == 0:
        return []  # avoid IndexError later on
    split_points = np.where(np.diff(true_indices) != 1)[0] + 1
    split_indices = np.concatenate([[0], split_points, [true_indices.size]])
    return [
        slice(true_indices[start], true_indices[end - 1])
        for start, end in zip(split_indices[:-1], split_indices[1:])
    ]


def filter_data(snow_data, sections_to_filter, window_length=5, polyorder=1):
    """
    Apply a Savitzky-Golay filter to specified sections of data.

    Args:
        snow_data (np.array): 1D array of snow cover data to filter.
        sections_to_filter (list of slices): Sections of the data array to filter.
        window_length (int): The length of the filter window (number of coefficients). window_length must be a positive odd integer.
        polyorder (int): The order of the polynomial used to fit the samples. polyorder must be less than window_length.

    Returns:
        np.array: The filtered data.
    """
    filtered_data = snow_data.copy()
    for section in sections_to_filter:
        if (section.stop - section.start) < window_length:
            # skip filtering for sections smaller than the window length
            continue
        filtered_data[section] = savgol_filter(
            snow_data[section], window_length, polyorder
        )
    return filtered_data


def fill_obscured_values_with_adjacent_observations(snow_data, sections_to_fill):
    """
    Fill missing observations (cloud, night) with adjacent valid observations using the median time index of the obscured period as the midpoint in the filling step-function.

    Args:
        snow_data (np.array): 1D array of snow cover data to fill.
        sections_to_fill (list of slices): Sections of the data array to fill.

    Returns:
        np.array: The filled data.
    """
    filled_data = snow_data.copy()
    for section in sections_to_fill:
        last_valid_before = snow_data[section.start - 1]

        # get first valid observation after obscured period is over
        first_valid_after = snow_data[section.stop]

        # get median index (halfway point) of the obscured period
        halfway_point = (section.start + section.stop) // 2

        # fill first half of obscured period with last valid observation before obscured onset
        filled_data[section.start : halfway_point] = last_valid_before

        # fill second half of obscured period with first valid observation afterward
        filled_data[halfway_point : section.stop + 1] = first_valid_after

    return filled_data


def construct_result(original_data, modified_data, mask):
    """
    Combine modified and original data based on the mask.

    Args:
        original_data (np.array): The original data array.
        modified_data (np.array): The filtered or filled data array.
        mask (np.array): The mask array indicating filtered or filled sections.

    Returns:
        np.array: The combined data array.
    """
    result = np.where(mask, modified_data, original_data)
    return result


def apply_filter_and_fill_to_masked_sections(
    snow_data, mask_data, window_length=5, polyorder=1
):
    def filter_section(data, filter_mask):
        sections_to_filter = identify_sections(filter_mask)
        filtered_data = filter_data(data, sections_to_filter, window_length, polyorder)
        return construct_result(data, filtered_data, filter_mask)

    def fill_section(data, obscured_mask):
        sections_to_filter = identify_sections(obscured_mask)
        filled_data = fill_obscured_values_with_adjacent_observations(
            data, sections_to_filter
        )
        return construct_result(data, filled_data, obscured_mask)

    snow_data_dtype = snow_data.dtype
    # first pass the Savitzky-Golay filter over the low illumination observations
    filtered_snow_data = xr.apply_ufunc(
        filter_section,
        snow_data,
        mask_data,
        input_core_dims=[["time"], ["time"]],
        output_core_dims=[["time"]],
        vectorize=True,
        dask="parallelized",
        output_dtypes=[snow_data_dtype],
    )

    # next forward/backward fill the night period
    night_filled_snow_data = xr.apply_ufunc(
        fill_section,
        filtered_snow_data,
        filtered_snow_data == inv_cgf_codes["Night"],
        input_core_dims=[["time"], ["time"]],
        output_core_dims=[["time"]],
        vectorize=True,
        dask="parallelized",
        output_dtypes=[snow_data_dtype],
    )

    # finally, forward/backward fill the cloud period
    cloud_filled_snow_data = xr.apply_ufunc(
        fill_section,
        night_filled_snow_data,
        night_filled_snow_data == inv_cgf_codes["Cloud"],
        input_core_dims=[["time"], ["time"]],
        output_core_dims=[["time"]],
        vectorize=True,
        dask="parallelized",
        output_dtypes=[snow_data_dtype],
    )
    return cloud_filled_snow_data


if __name__ == "__main__":
    log_file_path = os.path.join(os.path.expanduser("~"), "filter_and_fill.log")
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', filename=log_file_path, level=logging.INFO)
    parser = argparse.ArgumentParser(
        description="Script to filter data where low illumination conditions are present and to fill data gaps produced by Cloud or Night conditions."
    )
    parser.add_argument("tile_id", type=str, help="VIIRS Tile ID (ex. h11v02)")
    args = parser.parse_args()
    tile_id = args.tile_id
    client = Client()
    print("Monitor the Dask client dashboard for progress.")
    print(client.dashboard_link)

    fp = preprocessed_dir / f"snow_year_{SNOW_YEAR}_{tile_id}.nc"
    snow_ds = open_preprocessed_dataset(
        fp, {"x": "auto", "y": "auto"}, "CGF_NDSI_Snow_Cover", decode_coords="all"
    )
    bitflag_ds = open_preprocessed_dataset(
        fp, {"x": "auto", "y": "auto"}, "Algorithm_Bit_Flags_QA"
    )
    snow_valid_is_true = is_snow_valid_and_nonzero(snow_ds)
    low_illumination_is_true = is_low_illumination_for_solar_zenith(bitflag_ds)
    # function that opens this is context managed, but I'm paranoid
    bitflag_ds.close()
    value_to_filter_is_true = snow_valid_is_true & low_illumination_is_true
    mask_data = value_to_filter_is_true > 0

    filtered_and_filled_data = apply_filter_and_fill_to_masked_sections(
        snow_ds, mask_data, window_length=5, polyorder=1
    ).compute()
    if snow_ds.rio.crs:
        filtered_and_filled_data.rio.write_crs(snow_ds.rio.crs, inplace=True)
    snow_ds.close()  # expect context, but still paranoid so manually closing

    filtered_and_filled_data.name = "CGF_NDSI_Snow_Cover"
    logging.info(f"Writing {preprocessed_dir / f"snow_year_{SNOW_YEAR}_{tile_id}_filtered_filled.nc"}...")
    write_single_tile_xrdataset(filtered_and_filled_data, tile_id, "filtered_filled")

    client.close()
    print("Filtering and filling Complete.")
