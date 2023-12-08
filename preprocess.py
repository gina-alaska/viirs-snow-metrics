"""Preprocess the downloaded VIIRS GeoTIFFs to a time-indexed netCDF dataset that represents all data for a single snow year."""

import argparse
import logging
import pickle
from datetime import datetime, timedelta
from pathlib import Path

import xarray as xr
import rasterio as rio
import rioxarray
import numpy as np
import pandas as pd
import dask
import dask.array as da

from config import INPUT_DIR, SNOW_YEAR, preprocessed_dir
from luts import data_variables


def list_input_files(src_dir):
    """List all .tif files in the source directory.

    Args:
       src_dir (Path): The source directory containing the .tif files.

    Returns:
       list: A list of all .tif files in the source directory.
    """
    fps = [x for x in src_dir.glob("*.tif")]
    logging.info(f"$INPUT_DIR file count is {len(fps)}.")
    logging.info(f"Files that will be included in dataset: {fps}.")
    return fps


def parse_tile(fp):
    """Parse the tile from the filename.

    Args:
       fp (Path): The file path object.

    Returns:
       str: The tile extracted from the filename.
    """
    return fp.name.split("_")[2]


def parse_date(fp):
    """Parse the date from the filename.
    Args:
       fp (Path): The file path object.

    Returns:
       str: The date (DOY format) extracted from the filename.
    """
    return fp.name.split("_")[1][1:]


def parse_data_variable(fp):
    return fp.name.split("2D_")[1].split(".")[0][:-9]


def parse_satellite(fp):
    return fp.name.split("_")[0]


def convert_yyyydoy_to_date(doy_str):
    # Convert YYYY-DOY to a datetime object
    year, doy = int(doy_str[0:4]), int(doy_str[-3:])
    date = datetime(year, 1, 1) + timedelta(days=doy - 1)
    return date.date()


def construct_file_dict(fps):
    di = dict()
    for fp in fps:
        tile = parse_tile(fp)
        data_var = parse_data_variable(fp)
        if tile not in di:
            di[tile] = {}
        if data_var not in di[tile]:
            di[tile][data_var] = []
        di[tile][data_var].append(fp)
    # persist file dict as pickle for later reference
    with open("file_dict.pickle", "wb") as handle:
        pickle.dump(di, handle, protocol=pickle.HIGHEST_PROTOCOL)
    return di


def initialize_transform(geotiff):
    with rio.open(geotiff) as src:
        transform = src.transform
    return transform


def initialize_latlon(geotiff):
    with rio.open(geotiff) as src:
        lon, lat = np.meshgrid(
            np.linspace(src.bounds.left, src.bounds.right, src.meta["width"]),
            np.linspace(src.bounds.bottom, src.bounds.top, src.meta["height"]),
        )
    return lon, lat


def initialize_crs(geotiff):
    with rio.open(geotiff) as src:
        return src.crs


def make_sorted_raster_stack(files, yyyydoy_strings):
    # create an in-memory raster stack
    sorted_files = []
    for yyyydoy in yyyydoy_strings:
        for f in files:
            if yyyydoy in str(f): # would use if yyyydoy in parse_date(f) or == parse_date(f)
                sorted_files.append(f)

    raster_stack = []
    for file in sorted_files:
        with rio.open(file) as src:
            raster_stack.append(src.read(1))
    return raster_stack


def create_single_tile_dataset(tile_di, tile):
    # assuming all files have same metadata, use first one to get metadata
    reference_geotiff = tile_di[tile]["CGF_NDSI_Snow_Cover"][0]
    transform = initialize_transform(reference_geotiff)
    lon, lat = initialize_latlon(reference_geotiff)
    crs = initialize_crs(reference_geotiff)

    # timestamps are indentical across variables
    dates = [
        convert_yyyydoy_to_date(parse_date(x))
        for x in tile_di[tile]["CGF_NDSI_Snow_Cover"]
    ]
    # can we use the parse_date function to skip the prepending of 'A'?
    dates.sort()
    yyyydoy_strings = [str("A" + d.strftime("%Y") + d.strftime("%j")) for d in dates]
    #
    ds_dict = dict()
    ds_coords = {
        "time": pd.DatetimeIndex(dates),
        "x": lon[0, :],
        "y": lat[:, 0],
    }

    # CP note: use CGF snow [data_variables[2]] for testing
    # for data_var in [data_variables[2]]:
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


def write_tile_dataset(ds, tile):
    filename = preprocessed_dir / f"snow_year_{SNOW_YEAR}_{tile}.nc"
    ds.to_netcdf(filename)
    logging.info(f"NetCDF dataset for tile {tile} wriiten to {filename}.")


if __name__ == "__main__":
    logging.basicConfig(filename="preprocess.log", level=logging.INFO)

    parser = argparse.ArgumentParser(description="Preprocessing Script")
    parser.add_argument("tile_id", type=str, help="MODIS/VIIRS Tile ID (ex. h11v02)")
    args = parser.parse_args()
    tile_id = args.tile_id

    logging.info(f"Creating dataset for tile {tile_id}.")
    geotiffs = list_input_files(INPUT_DIR)
    geotiff_di = construct_file_dict(geotiffs)

    tile_ds = create_single_tile_dataset(geotiff_di, tile_id)
    write_tile_dataset(tile_ds, tile_id)
    print("Preprocessing Script Complete.")
