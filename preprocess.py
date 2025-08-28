"""Preprocess the downloaded VIIRS GeoTIFFs to a time-indexed netCDF dataset that represents all data for a single snow year."""

import argparse
import logging
import pickle
import os


import xarray as xr
import rasterio as rio
import numpy as np
import pandas as pd
import rioxarray
import dask.array as da

from config import (
    snow_year_input_dir,
    snow_year_scratch_dir,
)
from luts import data_variables
from shared_utils import (
    parse_tile,
    list_input_files,
    write_single_tile_xrdataset,
    convert_yyyydoy_to_date,
    construct_file_dict_h5,
    extract_coords_from_viirs_snow_h5,
    create_proj_from_viirs_snow_h5,
    get_attrs_from_h5,
    parse_date_h5,
    make_sorted_h5_stack,
)


def parse_date(fp):
    """Parse the date from the filename.
    Args:
       fp (Path): The file path object.

    Returns:
       str: The date (DOY format) extracted from the filename.
    """
    return fp.name.split("_")[1][1:]


def parse_data_variable(fp):
    """Parse the data variable from the filename.

    Args:
      fp (Path): The file path object.

    Returns:
      str: The data variable (e.g. 'CGF_NDSI_Snow_Cover') extracted from the filename.
    """
    return fp.name.split("2D_")[1].split(".")[0][:-9]


def parse_satellite(fp):
    """Parse the satellite from the filename. This function is not used at the moment, but may be used in the future to distiguish the VIIRS sensors that are aboard different satellites including Suomi NPP, NOAA-20, NOAA-21, and satellites to be launched in the future.

    Args:
       fp (Path): The file path object.

    Returns:
       str: Satellite extracted from the filename.
    """
    return fp.name.split("_")[0]


def construct_file_dict(fps):
    """Construct a dict mapping tiles and data variables to file paths.

    Args:
       fps (list): A list of file paths.

    Returns:
       (dict): hierarchical dict with keys of tile>>>data_variable that map to the file paths of the downloaded GeoTIFFs.
    """
    di = dict()
    for fp in fps:
        tile = parse_tile(fp)
        data_var = parse_data_variable(fp)
        if tile not in di:
            di[tile] = {}
        if data_var not in di[tile]:
            di[tile][data_var] = []
        di[tile][data_var].append(fp)
    # persist the dict as pickle for later reference
    with open(snow_year_scratch_dir / "file_dict.pickle", "wb") as handle:
        pickle.dump(di, handle, protocol=pickle.HIGHEST_PROTOCOL)
    return di


def initialize_transform(geotiff):
    """Initialize the affine transform from a reference GeoTIFF.

    Args:
       geotiff (Path): Path to the reference GeoTIFF.

    Returns:
       transform: The Affine transform object from the reference GeoTIFF.
    """
    with rio.open(geotiff) as src:
        transform = src.transform
    return transform


def initialize_latlon(geotiff):
    """Initialize latitude and longitude for xr.DataSet dimensions from the boundaries and shape of a reference GeoTIFF.

    Args:
       geotiff (Path): Path to a reference GeoTIFF.

    Returns:
       tuple: (longitude, latitude) values.
    """
    with rio.open(geotiff) as src:
        lon, lat = np.meshgrid(
            np.linspace(src.bounds.left, src.bounds.right, src.meta["width"]),
            np.linspace(src.bounds.bottom, src.bounds.top, src.meta["height"]),
        )
    return lon, lat


def initialize_crs(geotiff):
    """Initialize the coordinate reference system from metadata of a reference GeoTIFF.

    Args:
       geotiff (Path): Path to a GeoTIFF.

    Returns:
       crs: CRS from the reference GeoTIFF.
    """
    with rio.open(geotiff) as src:
        return src.crs


def make_sorted_raster_stack(files, yyyydoy_strings):
    """Create an in-memory raster stack sorted by date.

    This function takes a list of file paths and a list of chronological (pre-sorted)dates in YYYY-DOY format. It first creates a list of files that match the dates in the list. Then, it opens each of these files, reads the raster data from them, and appends it to the raster stack.

    Args:
       files (list): list of file paths.
       yyyydoy_strings (list): chronologically sorted dates in YYYY-DOY format.

      Returns:
         list: list of rasters (i.e. a stack) sorted by date.
    """
    # create an in-memory raster stack
    sorted_files = []
    for yyyydoy in yyyydoy_strings:
        for f in files:
            if yyyydoy == parse_date(f):
                sorted_files.append(f)

    raster_stack = []
    for file in sorted_files:
        with rio.open(file) as src:
            raster_stack.append(src.read(1))
    return raster_stack


def create_single_tile_dataset(tile_di, tile):
    """Create a time-indexed netCDF dataset for an entire snow year's worth of data for a single VIIRS tile.

    Args:
       tile_di (dict): A dictionary mapping tiles and data variables to file paths.
       tile (str): The tile to create the dataset for.

    Returns:
       xarray.Dataset: The single-tile dataset.
    """
    # assuming all files have same metadata, use first for metadata
    reference_geotiff = tile_di[tile]["CGF_NDSI_Snow_Cover"][0]
    transform = initialize_transform(reference_geotiff)
    lon, lat = initialize_latlon(reference_geotiff)
    crs = initialize_crs(reference_geotiff)

    # timestamps are indentical across variables, use snowcover
    dates = [
        convert_yyyydoy_to_date(parse_date(x))
        for x in tile_di[tile]["CGF_NDSI_Snow_Cover"]
    ]
    dates.sort()
    yyyydoy_strings = [d.strftime("%Y") + d.strftime("%j") for d in dates]

    ds_dict = dict()
    ds_coords = {
        "time": pd.DatetimeIndex(dates),
        "x": lon[0, :],
        "y": lat[:, 0],
    }

    # CP note: if testing just use CGF snow [data_variables[1]]
    for data_var in data_variables:
        logging.info(f"Stacking data for {data_var}...")
        raster_stack = make_sorted_raster_stack(
            tile_di[tile][data_var], yyyydoy_strings
        )
        data_var_dict = {data_var: (["time", "y", "x"], da.array(raster_stack))}
        ds_dict.update(data_var_dict)

    logging.info(f"Creating dataset...")
    ds = xr.Dataset(ds_dict, coords=ds_coords)
    logging.info(f"Assigning {crs} as dataset CRS...")
    ds.rio.write_crs(crs, inplace=True)
    logging.info(f"Assigning {transform} as dataset transform...")
    ds.rio.set_spatial_dims(x_dim="x", y_dim="y", inplace=True)
    ds.rio.write_transform(transform, inplace=True)
    return ds


def create_single_tile_dataset_from_h5(tile_di, tile):
    """Create a time-indexed netCDF dataset for an entire snow year's worth of data for a single VIIRS tile.

    Args:
       tile_di (dict): A dictionary mapping tiles and data variables to file paths.
       tile (str): The tile to create the dataset for.

    Returns:
       xarray.Dataset: The single-tile dataset.
    """
    # assuming all files have same metadata, use first for metadata
    reference_h5 = tile_di[tile][0]
    x_dim, y_dim = extract_coords_from_viirs_snow_h5(reference_h5)
    # transform = initialize_transform_h5(x_dim, y_dim)
    crs = create_proj_from_viirs_snow_h5(get_attrs_from_h5(reference_h5))

    dates = [convert_yyyydoy_to_date(parse_date_h5(x)) for x in tile_di[tile]]
    dates.sort()
    yyyydoy_strings = [d.strftime("%Y") + d.strftime("%j") for d in dates]

    ds_dict = dict()
    ds_coords = {
        "time": pd.DatetimeIndex(dates),
        "x": x_dim.values,
        "y": y_dim.values,
    }

    for data_var in data_variables:
        # logging.info(f"Stacking data for {data_var}...")
        variable_path = rf"/HDFEOS/GRIDS/VIIRS_Grid_IMG_2D/Data Fields/{data_var}"
        # Use lazy=True in make_sorted_h5_stack to return a stck of dask arrays, and the line below to restack them
        # Not yet configured to work with writing to NetCDF, but would be faster if possible to fix that
        h5_stack = make_sorted_h5_stack(tile_di[tile], yyyydoy_strings, variable_path)
        data_var_dict = {data_var: (["time", "y", "x"], da.array(h5_stack))}
        # data_var_dict = {data_var: (["time", "y", "x"], da.stack(h5_stack, axis=0).rechunk({0: len(h5_stack)}))}
        ds_dict.update(data_var_dict)

    ds = xr.Dataset(ds_dict, coords=ds_coords)

    ds.rio.write_crs(crs, inplace=True)
    ds.rio.set_spatial_dims(x_dim="x", y_dim="y", inplace=True)
    # ds.rio.write_transform(transform, inplace=True)

    return ds


def main(tile_id, format="tif"):

    logging.info(
        f"Creating preprocessed dataset for tile {tile_id} (format: {format})..."
    )

    if format == "tif":
        geotiffs = list_input_files(snow_year_input_dir)
        if not geotiffs:
            logging.error(f"No GeoTIFF files found in {snow_year_input_dir}. Exiting.")
            return
        geotiff_di = construct_file_dict(geotiffs)
        tile_ds = create_single_tile_dataset(geotiff_di, tile_id)
        write_single_tile_xrdataset(tile_ds, tile_id)
    else:
        h5_paths = list_input_files(snow_year_input_dir, extension="*.h5")
        if not h5_paths:
            logging.error(f"No HDF5 files found in {snow_year_input_dir}. Exiting.")
            return
        h5_dict = construct_file_dict_h5(h5_paths)
        tile_ds = create_single_tile_dataset_from_h5(h5_dict, tile_id)
        write_single_tile_xrdataset(tile_ds, tile_id)

    logging.info(f"Creating preprocessed dataset for tile {tile_id} complete.")


if __name__ == "__main__":
    log_file_path = os.path.join(os.path.expanduser("~"), "datacube_preprocess.log")
    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(message)s",
        # filename=log_file_path,
        level=logging.INFO,
    )
    parser = argparse.ArgumentParser(description="Preprocessing Script")
    parser.add_argument("tile_id", type=str, help="VIIRS Tile ID (ex. h11v02)")
    parser.add_argument(
        "--format",
        "-f",
        choices=["tif", "h5"],
        default="h5",
        help="Download/input File format: Older processing methods use tif, newer uses h5",
    )
    args = parser.parse_args()
    main(args.tile_id, args.format)
