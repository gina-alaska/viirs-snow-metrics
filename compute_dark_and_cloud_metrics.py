"""Script for analyzing CGF snow cover values during periods of winter darkness and cloud cover. This script is required for understanding how snow conditions are impacted by these uncertainty sources. Running this script in a main thread will produce a series of tagged GeoTIFFs that can be used to visualize the results in a GIS application. Otherwise, functionality can be imported and used in the actual snow metric computation to identify and filter CGF snow cover values as necessary."""

import logging
import argparse
import os

import numpy as np
import xarray as xr
import dask
from dask.distributed import Client

from config import preprocessed_dir, mask_dir, uncertainty_dir, SNOW_YEAR
from luts import inv_cgf_codes
from shared_utils import (
    open_preprocessed_dataset,
    apply_threshold,
    fetch_raster_profile,
    apply_mask,
    write_tagged_geotiff,
)


def is_obscured(chunked_cgf_snow_cover, dark_source):
    """Determine if a grid cell is obscured by a 'dark' condition.

    The 'dark' is a cloud or winter darkness condition. These uncertainty sources will be handled in an identical fashion. Maybe there is a better word than dark (penumbra???) to describe this condition, but I can't think of it right now.

    Args:
        chunked_cgf_snow_cover (xr.DataArray): chunked CGF snow cover data.
        dark_source (str): one of 'Cloud' or 'Night'.
    Returns:
        xr.DataArray: Obscured grid cells.
    """
    dark_on = chunked_cgf_snow_cover == inv_cgf_codes[dark_source]
    return dark_on


def count_darkness(dark_on):
    """Count the per-pixel occurrence of polar/winter darkness or cloud cover (in the initialization period) in the snow year.

    Args:
        dark_on (xr.Dataset): Obscured grid cells.

    Returns:
        xarray.DataArray: count of 'Cloud' or 'Night' values.
    """

    logging.info("Counting occurence of dark values...")
    darkness_count = dark_on.sum(dim="time")
    return darkness_count


def get_first_obscured_occurence_index(dark_on):
    """Get the first occurrence of the obscured condition.

    Returns the time index of the first chronological occurrence of the obscured condition. Ranges between 0 and 365.
    Args:
        dark_on (xr.DataArray): Obscured grid cells.
    Returns:
        xr.DataArray: The first occurrence of the obscured condition."""
    first_dark_index = dark_on.argmax(dim="time")
    return first_dark_index


def get_last_obscured_occurence_index(dark_on):
    """Get the last occurrence of the obscured condition.

    Returns the time index of the last chronological occurrence of the obscured condition. Ranges between 0 and 365.
    Args:
        dark_on (xr.DataArray): Obscured grid cells.
    Returns:
        xr.DataArray: The last occurrence of the obscured condition."""
    dark_on_reverse_time = dark_on.isel(time=slice(None, None, -1))
    last_occurrence_reverse = dark_on_reverse_time.argmax(dim="time")
    last_dark_index = dark_on.time.size - last_occurrence_reverse - 1
    return last_dark_index


def get_dusk_index(first_dark_index):
    """Get the time index of the last observation just before the onset of the obscured condition.

    Ranges between 0 and 364 (or 365 in a leap year). A value of 0 likely indicates that a grid cell did not experience the obscured condition at any point in the snow year.

    Args:
        first_dark_index (xr.DataArray): The first occurrence of the obscured condition.
    Returns:
        xr.DataArray: The index of the last observation before the onset of the obscured condition.
    """
    dusk_index = first_dark_index - 1
    # handle edge case where obscured condition did not occur at all
    # if we don't do this we hit an IndexError i.e. try to slice value at time index -1
    dusk_index = dusk_index.where(dusk_index >= 0, 0)
    return dusk_index


def get_dawn_index(last_dark_index, len_time_index):
    """Get the index of the first observation immediately after the obscured period.

    Index values range between 1 and 364 (or 365 in a leap year).

    Args:
        last_dark_index (xr.DataArray): The final occurrence of the obscured condition.
        len_time_index (int): The length of the time index for any given snow year used to cap the dawn index value.
    Returns:
        xr.DataArray: The index of the first observation following the obscured period.
    """
    dawn_index = last_dark_index + 1
    last_time_index = len_time_index - 1
    # handle edge case where obscured condition did not occur at all
    # if we don't do this we hit an IndexError i.e. try to slice value at time index 366
    dawn_index = dawn_index.where(dawn_index <= last_time_index, last_time_index)
    return dawn_index


def get_median_obscured_index(dusk_index, dawn_index):
    """Get the median index of the obscured period.

    Basically - what is the midpoint of the dark or cloud period? We'll use this as a basis to fill values in the obscured period and determine first/last days of the full snow season.

    Args:
        dusk_index (xr.DataArray): The index of the last observation before the onset of the obscured condition.
        dawn_index (xr.DataArray): The index of the first observation following the obscured period.
    Returns:
        xr.DataArray: The median index of the obscured period.
    """
    return (dusk_index + dawn_index) // 2


@dask.delayed
def get_dusk_observation(dusk_index, chunked_cgf_snow_cover):
    """Get the value of the snow cover product at the onset of the obscured condition.

    We delay this computation because index slicing the snowcover value at the time index basically requires an integer value.

    Args:
        chunked_cgf_snow_cover (xr.DataArray): chunked CGF snow cover data.
        dusk_index (xr.DataArray): index of the last observation before the onset of the obscured condition.
    Returns:
        xr.DataArray: Value of observation prior to the obscured period.
    """
    dusk_observation = chunked_cgf_snow_cover.isel(time=dusk_index)
    return dusk_observation


@dask.delayed
def get_dawn_observation(dawn_index, chunked_cgf_snow_cover):
    """Get the value of the snow cover product immediately after the obscured condition.

    We delay this computation because index slicing the snowcover value at the time index basically requires an integer value.

    Args:
        chunked_cgf_snow_cover (xr.DataArray): chunked CGF snow cover data.
        dawn_index (xr.DataArray): index of the first observation following the obscured period.
    Returns:
        xr.DataArray: value of the first observation following the obscured period.
    """
    dawn_observation = chunked_cgf_snow_cover.isel(time=dawn_index)
    return dawn_observation


def is_snow_on_at_dusk(dusk_observation):
    """Determine if snow is "on" at dusk.

    Args:
        dusk_observation (xr.DataArray): Value of the last observation before the onset of the obscured condition.
    Returns:
        xr.DataArray: boolean array indicating if snow is on at dusk.
    """
    return apply_threshold(dusk_observation)


def is_snow_on_at_dawn(dawn_observation):
    """Determine if snow is "on" at dawn.

    Args:
        dawn_observation (xr.DataArray): value of the first observation following the obscured period.
    Returns:
        xr.DataArray: boolean array indicating if snow is on at dawn.
    """
    return apply_threshold(dawn_observation)


@dask.delayed
def get_snow_transition_cases(snow_is_on_at_dusk, snow_is_on_at_dawn):
    """Determine the snow transition case during the obscured period.

    We delay this computation because `np.select` requires boolean arrays. Determine what, if anything, happened to the binary (on or off) snow state between the dusk observation the dawn observation. This information will be used to map different forward and backward data filling strategies for cloud or winter darkness conditions.

    Args:
        snow_is_on_at_dusk (xr.DataArray): boolean indicating if snow is on at dusk.
        snow_is_on_at_dawn (xr.DataArray): boolean indicating if snow is on at dawn.
    Returns:
        xr.DataArray: integer array indicating the snow transition case during the obscured period. 1 = no change, 2 = snow flipped on, 3 = snow flipped off.
    """
    # no change in snow condition during the obscured period
    # return true when conditions are identical before and after obscured period
    snow_did_not_flip = ~(snow_is_on_at_dusk ^ snow_is_on_at_dawn)
    # first snow happened during the obscured period
    snow_flipped_off_to_on = (snow_is_on_at_dusk == False) & (
        snow_is_on_at_dawn == True
    )
    # snow went away during the obscured period
    snow_flipped_on_to_off = (snow_is_on_at_dusk == True) & (
        snow_is_on_at_dawn == False
    )
    # return an array where the value is 1 if snow did not flip
    # or 2 if snow flipped off to on
    # or 3 if snow flipped on to off
    transition_cases = [
        snow_did_not_flip,
        snow_flipped_off_to_on,
        snow_flipped_on_to_off,
    ]
    # mapped to the above
    numeric_transition_cases = [1, 2, 3]
    # snow_transition_cases = np.select(
    #    transition_cases, numeric_transition_cases, default=0
    # )
    snow_transition_cases = xr.where(
        snow_did_not_flip,
        1,
        xr.where(snow_flipped_off_to_on, 2, xr.where(snow_flipped_on_to_off, 3, 0)),
    )
    return snow_transition_cases


def create_dark_metric_computation(dark_tag, dark_is_on, chunky_ds, tag_prefix=None):
    """Create a dictionary of delayed computations for darkness metrics.

    This is a look-up-table for how each metric gets computed.

    Args:
        dark_tag (str): One of 'cloud' or 'night'.
        dark_is_on (xr.DataArray): The obscured condition.
        chunky_ds (xr.Dataset): The chunked CGF snow cover data.
    Returns:
        dict: A dictionary of delayed computations for darkness metrics."""

    dark_metrics = dict()
    if tag_prefix is not None:
        dark_tag = f"{tag_prefix}_{dark_tag}"
    dark_metrics.update({f"{dark_tag}_obscured_day_count": count_darkness(dark_is_on)})
    dark_metrics.update(
        {
            f"dusk_index_of_last_obs_prior_to_{dark_tag}": get_dusk_index(
                get_first_obscured_occurence_index(dark_is_on)
            )
        }
    )
    dark_metrics.update(
        {
            f"dawn_index_of_first_obs_after_{dark_tag}": get_dawn_index(
                get_last_obscured_occurence_index(dark_is_on),
                chunky_ds.time.size,
            )
        }
    )
    dark_metrics.update(
        {
            f"median_index_of_{dark_tag}_period": get_median_obscured_index(
                dark_metrics[f"dusk_index_of_last_obs_prior_to_{dark_tag}"],
                dark_metrics[f"dawn_index_of_first_obs_after_{dark_tag}"],
            )
        }
    )
    dark_metrics.update(
        {
            f"value_at_{dark_tag}_dusk": get_dusk_observation(
                dark_metrics[f"dusk_index_of_last_obs_prior_to_{dark_tag}"],
                chunky_ds,
            ),
        }
    )
    dark_metrics.update(
        {
            f"value_at_{dark_tag}_dawn": get_dawn_observation(
                dark_metrics[f"dawn_index_of_first_obs_after_{dark_tag}"],
                chunky_ds,
            ),
        }
    )
    dark_metrics.update(
        {
            f"snow_is_on_at_{dark_tag}_dusk": is_snow_on_at_dusk(
                dark_metrics[f"value_at_{dark_tag}_dusk"]
            )
        }
    )
    dark_metrics.update(
        {
            f"snow_is_on_at_{dark_tag}_dawn": is_snow_on_at_dawn(
                dark_metrics[f"value_at_{dark_tag}_dawn"]
            )
        }
    )
    dark_metrics.update(
        {
            f"snow_transition_cases_{dark_tag}": get_snow_transition_cases(
                dark_metrics[f"snow_is_on_at_{dark_tag}_dusk"],
                dark_metrics[f"snow_is_on_at_{dark_tag}_dawn"],
            )
        }
    )
    return dark_metrics


def write_dark_metric(
    dark_metric_name,
    computation_di,
    tile_id,
    combined_mask,
    out_profile=None,
):
    """Trigger the dark metric computation and write to disk with `write_tagged_geotiff`

    Args:
        dark_metric_name (str): name of the dark metric to compute, must be key of computation_di
        computation_di (dict): dict of computations generate with create_dark_metric_computation
    Returns:
        None: writes data to GeoTIFF file
    """

    dark_metric_arr = computation_di[dark_metric_name].compute()

    write_tagged_geotiff(
        uncertainty_dir,
        tile_id,
        "",
        dark_metric_name,
        out_profile,
        apply_mask(combined_mask, dark_metric_arr).astype("int16"),
    )
    return None


def main(tile_id, smoothed_input=None, format="h5"):
    logging.info(
        f"Computing winter darkness and cloud cover metrics for tile {tile_id}."
    )

    client = Client(memory_limit="64GiB", timeout="60s")  # mem per Dask worker

    if smoothed_input is not None:
        logging.info(f"Using smoothed input file: {smoothed_input}")
        fp = preprocessed_dir / f"snow_year_{SNOW_YEAR}_{tile_id}_{smoothed_input}.nc"
        ds = open_preprocessed_dataset(fp, {"x": "auto", "y": "auto"}).to_dataarray()[0]
        output_tag = smoothed_input
    else:
        fp = preprocessed_dir / f"snow_year_{SNOW_YEAR}_{tile_id}.nc"
        ds = open_preprocessed_dataset(
            fp,
            {"x": "auto", "y": "auto"},
            "CGF_NDSI_Snow_Cover",
        )
        output_tag = "raw"
    # intialize input and output parameters
    out_profile = fetch_raster_profile(
        tile_id, updates={"dtype": "int16", "nodata": 0}, format=format
    )

    combined_mask = mask_dir / f"{tile_id}_mask_combined_{SNOW_YEAR}.tif"

    writing_kwargs = {
        "tile_id": tile_id,
        "combined_mask": combined_mask,
        "out_profile": out_profile,
    }

    for darkness_source in ["Night", "Cloud"]:
        tag = darkness_source.lower()
        di_tag = f"{output_tag}_{darkness_source.lower()}"

        darkness_is_on = is_obscured(ds, darkness_source)
        dark_computation_di = create_dark_metric_computation(
            tag, darkness_is_on, ds, output_tag
        )

        # these are relatively rapid computes
        write_dark_metric(
            f"{di_tag}_obscured_day_count", dark_computation_di, **writing_kwargs
        )
        write_dark_metric(
            f"dusk_index_of_last_obs_prior_to_{di_tag}",
            dark_computation_di,
            **writing_kwargs,
        )
        write_dark_metric(
            f"dawn_index_of_first_obs_after_{di_tag}",
            dark_computation_di,
            **writing_kwargs,
        )
        write_dark_metric(
            f"median_index_of_{di_tag}_period", dark_computation_di, **writing_kwargs
        )
        # more memory intensive because they rely on computing the above indices
        write_dark_metric(
            f"value_at_{di_tag}_dusk", dark_computation_di, **writing_kwargs
        )
        write_dark_metric(
            f"value_at_{di_tag}_dawn", dark_computation_di, **writing_kwargs
        )
        write_dark_metric(
            f"snow_is_on_at_{di_tag}_dusk", dark_computation_di, **writing_kwargs
        )
        write_dark_metric(
            f"snow_is_on_at_{di_tag}_dawn", dark_computation_di, **writing_kwargs
        )
        write_dark_metric(
            f"snow_transition_cases_{di_tag}", dark_computation_di, **writing_kwargs
        )

    client.close()
    ds.close()
    print("Computation of cloud and darkness metrics complete.")


if __name__ == "__main__":
    log_file_path = os.path.join(os.path.expanduser("~"), "dark_and_cloud_metrics.log")
    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(message)s",
        filename=log_file_path,
        level=logging.INFO,
    )
    parser = argparse.ArgumentParser(
        description="Compute metrics for cloud and polar/winter darkness periods."
    )
    parser.add_argument("tile_id", type=str, help="VIIRS Tile ID (ex. h11v02)")
    parser.add_argument(
        "--format",
        "-f",
        choices=["tif", "h5"],
        default="h5",
        help="Download/input File format: Older processing methods use tif, newer uses h5",
    )
    # optional argument to compute metrics for a smoothed dataset
    parser.add_argument(
        "--smoothed_input", type=str, help="Suffix of smoothed input file."
    )
    args = parser.parse_args()
    main(tile_id=args.tile_id, smoothed_input=args.smoothed_input, format=args.format)
