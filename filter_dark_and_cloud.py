"""Filtering of winter darkness and cloud cover."""

import logging
import argparse

import rasterio as rio
import numpy as np
import xarray as xr
from dask.distributed import Client

from config import preprocessed_dir, mask_dir, uncertainty_dir, SNOW_YEAR
from luts import (
    inv_cgf_codes,
)
import compute_snow_metrics as csm
from shared_utils import (
    open_preprocessed_dataset,
    fetch_raster_profile,
    apply_mask,
    write_tagged_geotiff,
)


def is_obscured(chunked_cgf_snow_cover, dark_source):
    """Determine if a grid cell is obscured by a 'dark' condition.

    The 'dark' is a cloud or winter darkness condition. These uncertainty sources will be handled in an identical fashion. Maybe there is a better word than dark (penumbra???) to describe this condition, but I can't think of it right now.

    Args:
        chunked_cgf_snow_cover (xarray.DataArray): The chunked CGF snow cover data.
        dark_source (str): one of 'Cloud' or 'Night'.
    Returns:
        xr.DataArray: Obscured grid cells.
    """
    dark_on = chunked_cgf_snow_cover == inv_cgf_codes[dark_source]
    return dark_on


def count_darkness(chunked_cgf_snow_cover, dark_source):
    """Count the per-pixel occurrence of polar/winter darkness or cloud cover (in the initialization period) in the snow year.

    Args:
        ds_chunked (xarray.Dataset): The chunked dataset.
        dark_source (str): one of 'Cloud' or 'Night'.

    Returns:
        xarray.DataArray: count of 'Cloud' or 'Night' values.
    """

    logging.info(f"Counting occurence of `Night` values...")
    darkness_count = (chunked_cgf_snow_cover == inv_cgf_codes[dark_source]).sum(
        dim="time"
    )
    return darkness_count


def get_first_obscured_occurence_index(dark_on):
    """Get the first occurrence of the obscured condition.

    Returns the first chronological occurrence of the obscured condition in the time dimension. Ranges between 0 and 365.
    Args:
        dark_on (xr.DataArray): Obscured grid cells.
    Returns:
        xr.DataArray: The first occurrence of the obscured condition."""
    first_dark_index = dark_on.argmax(dim="time")
    # first_dark_index = np.argmax(dark_on)
    return first_dark_index


def get_last_obscured_occurence_index(dark_on):
    """Get the last occurrence of the obscured condition.

    Returns the last chronological occurrence of the obscured condition in the time dimension. Ranges between 0 and 365.
    Args:
        dark_on (xr.DataArray): Obscured grid cells.
    Returns:
        xr.DataArray: The last occurrence of the obscured condition."""
    dark_on_reverse_time = dark_on.isel(time=slice(None, None, -1))
    last_occurrence_reverse = dark_on_reverse_time.argmax(dim="time")
    last_dark_index = dark_on.time.size - last_occurrence_reverse - 1
    return last_dark_index


def get_dusk_index(first_dark_index):
    """Get the index of the last observation before the onset of the obscured condition.

    Ranges between -1 and 364. A value of -1 indicates that the first obscured condition occurred on the first day of the year. Again, 'dusk' might not be the best word but whatever.
    Args:
        first_dark_index (xr.DataArray): The first occurrence of the obscured condition.
    Returns:
        xr.DataArray: The index of the last observation before the onset of the obscured condition.
    """
    dusk_index = first_dark_index - 1
    return dusk_index


def get_dawn_index(last_dark_index):
    """Get the index of the last observation before the onset of the obscured condition.

    Ranges between 1 and 366. As above re the choice of the word 'dawn'.
    Args:
        last_dark_index (xr.DataArray): The final occurrence of the obscured condition.
    Returns:
        xr.DataArray: The index of the first observation following the obscured period.
    """
    dawn_index = last_dark_index + 1
    return dawn_index


def get_median_obscured_index(dusk, dawn):
    """Get the median index of the obscured period.

    Basically - what is the midpoint of the dark or cloud period? We'll use this as a basis to fill values in the obscured period and determine first/last days of the full snow season.

    Args:
        dusk (xr.DataArray): The index of the last observation before the onset of the obscured condition.
        dawn (xr.DataArray): The index of the first observation following the obscured period.
    Returns:
        xr.DataArray: The median index of the obscured period.
    """
    return (dusk + dawn) // 2


def get_snow_transition_case_dark(dusk, dawn, snow_on):
    """Determine the snow transition case during the obscured period.

    This functions determines what, if anything, happened to the binary snow-state between the last observation before the onset of the obscured condition and the first observation following the obscured period. It returns three boolean arrays: one for no change in snow condition, one for snow flipping from off to on, and one for snow flipping from on to off.

    Args:
        dusk (xr.DataArray): The index of the last observation before the onset of the obscured condition.
        dawn (xr.DataArray): The index of the first observation following the obscured period.
        snow_on (xr.DataArray): The binary snow-state.
    Returns:
        tuple: Three boolean arrays indicating no change in snow condition, snow flipping from off to on, and snow flipping from on to off.
    """
    # some handling for edge cases where the obscured (cloud or night) condition did not occur at all that can otherwise toss an IndexError when we try to grab the value of `snow_on` at time index -1 for example
    try:
        last_obs_before_darkness = snow_on.isel(time=dusk)
    except:
        # handle case where first_dark is first_day_of_year
        last_obs_before_darkness = snow_on.isel(time=0)
    try:
        first_obs_after_darkness = snow_on.isel(time=dawn)
    except:
        # handle case where last_dark is last_day_of_year
        first_obs_after_darkness = snow_on.isel(time=365)

    # no change in snow condition during the obscured period
    # return true when conditions are identical before and after obscured period
    snow_did_not_flip = ~(last_obs_before_darkness ^ first_obs_after_darkness)
    # first snow happened during the obscured period, this is probably common
    snow_flipped_off_to_on = (last_obs_before_darkness == False) & (
        first_obs_after_darkness == True
    )
    # snow went away during the obscured period, probably less common
    snow_flipped_on_to_off = (last_obs_before_darkness == True) & (
        first_obs_after_darkness == False
    )
    return (
        snow_did_not_flip.compute(),
        snow_flipped_off_to_on.compute(),
        snow_flipped_on_to_off.compute(),
    )


def compute_snow_darkness_transitions(chunked_cgf_snow_cover, obscured_source):
    """Compute snow transitions during the obscured period.

    This function orchestrates the computation of snow transitions during the obscured period. It returns three boolean arrays: one for no change in snow condition, one for snow flipping from off to on, and one for snow flipping from on to off. It also returns the dusk, dawn, and median obscured indices. These data will be used to map different forward and backward data filling strategies for cloudy or winter darkness conditions.

    Args:
        chunked_cgf_snow_cover (xarray.DataArray): The chunked CGF snow cover data.
    Returns:
        tuple: Six arrays in total. Three boolean arrays indicating no change in snow condition, snow flipping from off to on, and snow flipping from on to off. Also returns 'dusk', 'dawn', and median obscured indices.
    """

    dark_on = is_obscured(chunked_cgf_snow_cover, obscured_source)

    first_dark_index = get_first_obscured_occurence_index(dark_on)
    last_dark_index = get_last_obscured_occurence_index(dark_on)
    dusk = get_dusk_index(first_dark_index).compute()
    dawn = get_dawn_index(last_dark_index).compute()
    median_obscured_index = get_median_obscured_index(dusk, dawn)

    snow_is_on = csm.apply_threshold(chunked_cgf_snow_cover)
    get_snow_transition_case_dark(dusk, dawn, snow_is_on)
    snow_did_not_flip, snow_flipped_off_to_on, snow_flipped_on_to_off = (
        get_snow_transition_case_dark(dusk, dawn, snow_is_on)
    )
    return (
        snow_did_not_flip,
        snow_flipped_off_to_on,
        snow_flipped_on_to_off,
        dusk,
        dawn,
        median_obscured_index,
    )


if __name__ == "__main__":
    logging.basicConfig(filename="dark_and_cloud_filter.log", level=logging.INFO)
    parser = argparse.ArgumentParser(description="Cloud and Darkness Filtering Script")
    parser.add_argument("tile_id", type=str, help="VIIRS Tile ID (ex. h11v02)")
    args = parser.parse_args()
    tile_id = args.tile_id
    logging.info(f"Filtering winter darkness and cloud cover for tile {tile_id}.")

    client = Client()

    fp = preprocessed_dir / f"snow_year_{SNOW_YEAR}_{tile_id}.nc"
    chunky_ds = open_preprocessed_dataset(
        fp, {"x": "auto", "y": "auto"}, "CGF_NDSI_Snow_Cover"
    )
    out_profile = fetch_raster_profile(tile_id, {"dtype": "int16", "nodata": 0})
    combined_mask = mask_dir / f"{tile_id}__mask_combined_{SNOW_YEAR}.tif"
    for darkness_source in ["Night"]:  # add "Cloud" back in here
        # (
        #     snow_did_not_flip,
        #     snow_flipped_off_to_on,
        #     snow_flipped_on_to_off,
        #     dusk,
        #     dawn,
        #     median_obscured_index,
        # ) = compute_snow_darkness_transitions(chunky_ds, darkness_source)
        snow_darkness_transitions = compute_snow_darkness_transitions(
            chunky_ds, darkness_source
        )
        dark_count = count_darkness(chunky_ds, darkness_source)

        dark_label = darkness_source.lower()
        tiff_labels = [
            f"snow_did_not_flip_during_{dark_label}",
            f"snow_flipped_off_to_on_during_{dark_label}",
            f"snow_flipped_on_to_off_during_{dark_label}",
            f"dusk_index_of_last_obs_prior_to_{dark_label}",
            f"dawn_index_of_last_obs_prior_to_{dark_label}",
            f"{dark_label}_darkness_count_of_days",
        ]
        for label, arr in zip(
            tiff_labels, list(snow_darkness_transitions) + [dark_count]
        ):
            write_tagged_geotiff(
                uncertainty_dir,
                tile_id,
                "",
                label,
                out_profile,
                apply_mask(combined_mask, arr),
            )

    client.close()
    print("Cloud and Darkness Filtering Script Complete.")
