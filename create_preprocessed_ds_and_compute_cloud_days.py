import logging
import h5py
import os
import argparse

from config import preprocessed_dir, single_metric_dir, SNOW_YEAR, snow_year_input_dir
from luts import inv_cgf_codes, data_variables
from shared_utils import (
    open_preprocessed_dataset,
    fetch_raster_profile,
    write_tagged_geotiff,
    list_input_files,
    construct_file_dict_h5,
)
from preprocess import create_single_tile_dataset_from_h5, write_single_tile_xrdataset


def list_h5_data_fields(hdf_path):
    """
    Lists the variables in the Data Fields group of a VIIRS HDF5 file.

    Args:
        hdf_path (str): Path to the HDF5 file.

    Returns:
        list: List of variable names in the Data Fields group, or None if not found.
    """
    data_fields_path = "/HDFEOS/GRIDS/VIIRS_Grid_IMG_2D/Data Fields"
    with h5py.File(hdf_path, "r") as hdf:
        if data_fields_path in hdf:
            data_fields_group = hdf[data_fields_path]
            variables = list(data_fields_group.keys())
            return variables
        else:
            print(f"Path '{data_fields_path}' not found in the file.")
            return None


def count_cloud_occurence(chunked_cgf_snow_cover):
    """Count the per-pixel occurrence of "Cloud" in the snow year.

    Args:
        chunked_cgf_snow_cover (xarray.Dataset): The chunked dataset.

    Returns:
        xr.DataArray: count of "Cloud" values".
    """
    logging.info(f"Counting occurrence of `Cloud` values...")
    cloud_day_count = (chunked_cgf_snow_cover == inv_cgf_codes["Cloud"]).sum(dim="time")
    return cloud_day_count


def main(tile_id, source_data_type="raw"):
    """Main processing function to create cloud days metric for a single tile.

    This can be used on non-cloud-gap-filled data to see how many days were originally classified as cloudy

    Args:
        tile_id (str): The tile ID to process. Default is "h11v02".
    Returns:
        None

    """
    logging.basicConfig(level=logging.INFO)
    h5_paths = list_input_files(snow_year_input_dir, extension="*.h5")
    if not h5_paths:
        logging.error(f"No HDF5 files found in {snow_year_input_dir}. Exiting.")
        return

    logging.info(f"HDF5 Data Fields: {list_h5_data_fields(h5_paths[0])}")

    VNP10A1_variables = [
        "Algorithm_bit_flags_QA",
        "NDSI_Snow_Cover",
    ]

    if source_data_type == "raw":
        variables = VNP10A1_variables
    else:
        variables = data_variables

    h5_dict = construct_file_dict_h5(h5_paths)
    tile_ds = create_single_tile_dataset_from_h5(
        h5_dict, tile_id, data_variables=variables
    )
    write_single_tile_xrdataset(tile_ds, tile_id)

    unprocessed_fp = preprocessed_dir / f"snow_year_{SNOW_YEAR}_{tile_id}.nc"
    unprocessed_ds = open_preprocessed_dataset(
        unprocessed_fp,
        {"x": "auto", "y": "auto"},
        "NDSI_Snow_Cover",
    )
    snow_metrics = {"cloud_days": count_cloud_occurence(unprocessed_ds)}

    unprocessed_ds.close()
    single_metric_profile = fetch_raster_profile(
        tile_id, {"dtype": "int16", "nodata": 0}, format="GTiff"
    )
    for metric_name, metric_array in snow_metrics.items():
        write_tagged_geotiff(
            single_metric_dir,
            tile_id,
            "",
            metric_name,
            single_metric_profile,
            metric_array.compute().values.astype("int16"),
        )


if __name__ == "__main__":
    log_file_path = os.path.join(os.path.expanduser("~"), "cloud_metrics.log")
    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(message)s",
        # filename=log_file_path,
        level=logging.INFO,
    )
    parser = argparse.ArgumentParser(
        description="""Create preprocessed dataset and compute cloud days metric for a single VIIRS tile.
        This script is designed to be usable with either Cloud Gap Filled (i.e. VNP10A1F) or raw (i.e. VNP10A1 data).
        To run with raw data, recommend setting up a new set of working directories and using download.py with --short_name VNP10A1 to avoid overwriting existing data.
        """
    )
    parser.add_argument("tile_id", type=str, help="VIIRS Tile ID (ex. h11v02)")
    parser.add_argument(
        "--source_data",
        type=str,
        choices=["CGF", "raw"],
        default="raw",
        help="Source data type (must be 'CGF' or 'raw')",
    )

    args = parser.parse_args()
    main(tile_id=args.tile_id, source_data_type=args.source_data)
