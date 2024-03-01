"""Gather data to characterize uncertainty within each tile including cloud persistence, bowtie trim, no decision, etc."""

import argparse
import logging

from dask.distributed import Client

from config import SNOW_YEAR, preprocessed_dir, uncertainty_dir
from luts import inv_cgf_codes
from shared_utils import (
    open_preprocessed_dataset,
    fetch_raster_profile,
    write_tagged_geotiff,
)


def count_no_decision_occurence(ds_chunked):
    """Count the per-pixel occurrence of "no decision" in the snow year.

    Args:
        ds_chunked (xarray.Dataset): The chunked dataset.

    Returns:
        xarray.DataArray: count of "No decision" values".
    """

    logging.info(f"Counting occurence of `No decision` values...")
    no_decision_count = (ds_chunked == inv_cgf_codes["No decision"]).sum(dim="time")
    return no_decision_count


def count_missing_l1b_occurence(ds_chunked):
    """Count the per-pixel occurrence of "Missing L1B data" in the snow year.

    Args:
        ds_chunked (xarray.Dataset): The chunked dataset.

    Returns:
        xarray.DataArray: count of "Missing L1B data" values".
    """

    logging.info(f"Counting occurence of `Missing L1B data` values...")
    missing_l1b_count = (ds_chunked == inv_cgf_codes["Missing L1B data"]).sum(
        dim="time"
    )
    return missing_l1b_count


def count_l1b_calibration_fail(ds_chunked):
    """Count the per-pixel occurrence of "L1B data failed calibration" in the snow year.

    Args:
        ds_chunked (xarray.Dataset): The chunked dataset.

    Returns:
        xarray.DataArray: count of "L1B data failed calibration" values".
    """

    logging.info(f"Counting occurence of `L1B data failed calibration` values...")
    l1b_fail_count = (ds_chunked == inv_cgf_codes["L1B data failed calibration"]).sum(
        dim="time"
    )
    return l1b_fail_count


def count_bowtie_trim(ds_chunked):
    """Count the per-pixel occurrence of "Onboard VIIRS bowtie trim" in the snow year.

    Args:
        ds_chunked (xarray.Dataset): The chunked dataset.

    Returns:
        xarray.DataArray: count of "Onboard VIIRS bowtie trim" values".
    """

    logging.info(f"Counting occurence of `Onboard VIIRS bowtie trim` values...")
    bowtie_trim_count = (ds_chunked == inv_cgf_codes["Onboard VIIRS bowtie trim"]).sum(
        dim="time"
    )
    return bowtie_trim_count


def get_max_cloud_persistence(ds_chunked):
    """Determine maximum per-pixel cloud persistence value.

    Args:
        ds_chunked (xarray.Dataset): The chunked dataset of cloud persistence.

    Returns:
        xarray.DataArray: max cloud persistence value".
    """

    logging.info(f"Finding maximum cloud persistence value...")
    max_cloud_persist = ds_chunked.max(dim="time")
    return max_cloud_persist


def get_data_qa_screens(bitflag_integer):
    """Get the data QA screens from the bitflag integer.

    This is mostly a convenience function to make sense of what a value of "128" means, for example.

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

    This function helps identify candidate values for filtering.

    Args:
        snowcover_value (int): The snowcover value.
    Returns:
        bool: Whether the snowcover value is valid and nonzero.
    """
    return (snowcover_value > 1) & (snowcover_value <= 100)


if __name__ == "__main__":
    logging.basicConfig(filename="gather_uncertainty.log", level=logging.INFO)
    parser = argparse.ArgumentParser(
        description="Script to Fetch Data For Uncertainty Analysis"
    )
    parser.add_argument("tile_id", type=str, help="VIIRS Tile ID (ex. h11v02)")
    args = parser.parse_args()
    tile_id = args.tile_id
    logging.info(
        f"Gathering uncertainty data for tile {tile_id} for snow year {SNOW_YEAR}."
    )
    client = Client()
    uncertainty_data = dict()
    fp = preprocessed_dir / f"snow_year_{SNOW_YEAR}_{tile_id}.nc"

    cgf_snow_ds = open_preprocessed_dataset(
        fp, {"x": "auto", "y": "auto"}, "CGF_NDSI_Snow_Cover"
    )
    uncertainty_data.update({"no decision": count_no_decision_occurence(cgf_snow_ds)})
    uncertainty_data.update({"missing L1B": count_missing_l1b_occurence(cgf_snow_ds)})
    uncertainty_data.update({"L1B fail": count_l1b_calibration_fail(cgf_snow_ds)})
    uncertainty_data.update({"bowtie trim": count_bowtie_trim(cgf_snow_ds)})
    cgf_snow_ds.close()

    cloud_ds = open_preprocessed_dataset(
        fp, {"x": "auto", "y": "auto"}, "Cloud_Persistence"
    )
    uncertainty_data.update(
        {"max_cloud_persistence": get_max_cloud_persistence(cgf_snow_ds)}
    )
    cloud_ds.close()

    out_profile = fetch_raster_profile(tile_id, {"dtype": "int16", "nodata": 0})

    for uncertainty_name, uncertainty_array in uncertainty_data.items():
        if uncertainty_array.sum().compute() == 0:
            logging.info(
                f"No occurences found for {uncertainty_name}. A GeoTIFF will not be written."
            )
            continue
        write_tagged_geotiff(
            uncertainty_dir,
            tile_id,
            "",
            uncertainty_name,
            out_profile,
            uncertainty_array.compute().values.astype("int16"),
        )

    client.close()
    print("Uncertainty Data Fetch Complete.")
