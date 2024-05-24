"""Script for smoothing CGF snow cover values during periods of low illumination. Running this script in a main thread will produce a netCDF dataset of smoothed snow cover data."""

import logging
import argparse
import os

import numpy as np
import xarray as xr
import dask
from scipy.signal import savgol_filter
from dask.distributed import Client

from config import preprocessed_dir, SNOW_YEAR
from shared_utils import open_preprocessed_dataset, write_single_tile_xrdataset


def get_data_qa_screens(bitflag_integer):
    """Get the data QA screens from the bitflag integer.

    Mostly a convenience function to make sense of what a value of "128" means in the Bitflag QA data.

    Args:
        bitflag_integer (array-like with int dtype): The bitflag integer.
    Returns:
        str: The data QA screens.
    """
    # from USER GUIDE: VIIRS/[NPP|JPSS1] CGF Snow Cover Daily L3 Global 375m SIN Grid, Version 2
    conditions = {
        0: "Inland water screen",
        1: "Low visible screen failed, snow detection reversed to no snow",
        2: "Low NDSI screen failed, snow detection reversed to no snow",
        3: "Combined temperature/height screen failed",
        4: "spare",
        5: "High Shortwave IR (SWIR) reflectance screen",
        6: "spare",
        7: "Uncertain snow detection due to low illumination (solar zenith flag)",
    }

    # convert value to binary and pad with zeros to get 8 bits
    binary_value = format(bitflag_integer, "08b")

    # per the guide, bit positions are indexed from 'right' (0) to 'left' (7)
    # so reverse the binary string to match the bit order
    binary_value = binary_value[::-1]

    conditions_list = []
    for i in range(len(binary_value)):
        # bit default is off (0), so if 1 add the corresponding condition to the list
        if binary_value[i] == "1":
            conditions_list.append(conditions[i])

    # CP note: maybe delete below and retain list instead
    conditions_string = ", ".join(conditions_list)
    return conditions_string


def is_low_illumination_for_solar_zenith(bitflag_value):
    """Determine if the bitflag value indicates low illumination for solar zenith angles less than 70 degrees.

    Note that the bitflag can represent multiple conditions, e.g., 2**7 = 128 indicates low illumination, but so does 2**7 + 2^0 = 129 (low illumination condition and the 'Inland water screen' condition are both present). This function helps identify candidate values for filtering.

    Args:
        bitflag_value (int): The bitflag value.
    Returns:
        bool: Whether the bitflag value indicates low illumination due to solar zenith angles less than 70 degrees.
    """
    return (bitflag_value & 128) != 0


def is_snow_valid_and_nonzero(snowcover_value):
    """Determine if the snowcover value is valid and nonzero.

    This function helps identify candidate values for filtering. The assumption is that zero values are a strong "no snow" signal, so we want to exclude these values from being smoothed by the polynomial filter.

    Args:
        snowcover_value (int): The snowcover value.
    Returns:
        bool: Whether the snowcover value is valid and nonzero.
    """
    return (snowcover_value > 1) & (snowcover_value <= 100)


def smooth_low_illumination_observations(
    snow_cover, low_illumination_mask, snow_cover_mask, filter_window_size
):
    mask_to_filter = snow_cover_mask & low_illumination_mask
    streaks = np.where(mask_to_filter, 1, 0)
    # difference between consecutive elements in streaks
    diff = np.diff(streaks)
    # diff is 1 (the start of a streak, 0 to 1) or -1 (end of a streak, 1 to 0)
    start_indices = np.where(diff == 1)[0] + 1
    end_indices = np.where(diff == -1)[0]
    # copy the input array, otherwise xr.apply_ufunc will fail
    smoothed_snow_cover = snow_cover.copy()
    for start, end in zip(start_indices, end_indices):
        try:
            smoothed_snow_cover[start : end + 1] = savgol_filter(
                snow_cover[start : end + 1], filter_window_size, 1
            )
        except:
            smoothed_snow_cover[start : end + 1] = snow_cover[start : end + 1]
    return smoothed_snow_cover


def apply_smoothing_of_low_illumination_observations(
    snow_cover, low_illumination_mask, snow_cover_mask, filter_window_size
):
    smoothed_datacube = xr.apply_ufunc(
        smooth_low_illumination_observations,
        snow_cover,
        low_illumination_mask,
        snow_cover_mask,
        kwargs={"filter_window_size": filter_window_size},
        vectorize=True,
        input_core_dims=[["time"], ["time"], ["time"]],
        output_core_dims=[["time"]],
        dask="parallelized",
        output_dtypes=[np.int8],
    ).transpose("time", "y", "x")
    return smoothed_datacube


if __name__ == "__main__":
    log_file_path = os.path.join(os.path.expanduser("~"), "dark_and_cloud_metrics.log")
    logging.basicConfig(filename=log_file_path, level=logging.INFO)
    parser = argparse.ArgumentParser(
        description="Script to create smoothed snow cover data for low illumination conditions."
    )
    parser.add_argument("tile_id", type=str, help="VIIRS Tile ID (ex. h11v02)")
    parser.add_argument(
        "smoothing_window_size",
        type=int,
        help="Size of window to use to smooth low illumination observations. Must be an odd integer.",
    )
    args = parser.parse_args()
    assert (
        args.smoothing_window_size % 2 != 0
    ), "Smoothing window size must be an odd integer."
    tile_id = args.tile_id
    window_size = args.smoothing_window_size
    logging.info(
        f"Smoothing low illumination observations for tile {tile_id} with a smoothing window size of {window_size}."
    )

    client = Client(memory_limit="128GiB", timeout="180s")  # mem per Dask worker
    # intialize input and output parameters
    fp = preprocessed_dir / f"snow_year_{SNOW_YEAR}_{tile_id}.nc"
    ds = open_preprocessed_dataset(
        fp, {"x": "auto", "y": "auto"}, "CGF_NDSI_Snow_Cover"
    )

    # as above, but applied to the smoothed data
    smoothed_ds = apply_smoothing_of_low_illumination_observations(
        ds,
        is_low_illumination_for_solar_zenith(
            open_preprocessed_dataset(
                fp, {"x": "auto", "y": "auto"}, "Algorithm_Bit_Flags_QA"
            )
        ),
        is_snow_valid_and_nonzero(ds),
        window_size,
    )
    ds.close()
    write_single_tile_xrdataset(smoothed_ds, tile_id, "smoothed_low_illumination")
    client.close()
    print("Smoothing Complete.")
