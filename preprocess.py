import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path

import xarray as xr
import rasterio as rio
import rioxarray
import numpy as np
import pandas as pd

from config import INPUT_DIR, SCRATCH_DIR
from luts import data_variables

def list_input_files(src_dir):
    fps = [x for x in src_dir.glob("*.tif")]
    logging.info(f"$INPUT_DIR file is {len(fps)}.")
    return fps


def parse_tile(fp):
    return fp.name.split("_")[2]


def parse_date(fp):
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


def make_raster_stack(files):
    # Create an in-memory raster stack
    raster_stack = []
    for file in files:
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

    ds_dict = dict()
    ds_coords = {
        "time": pd.DatetimeIndex(dates),
        "x": lon[0, :],
        "y": lat[:, 0],
    }

    for data_var in [data_variables[2]]:  # only snow for testing
        raster_stack = make_raster_stack(tile_di[tile][data_var])
        data_var_dict = {data_var: (["time", "y", "x"], np.array(raster_stack))}
        ds_dict.update(data_var_dict)

    ds = xr.Dataset(ds_dict, coords=ds_coords)
    # Set the coordinate reference system
    ds.rio.write_crs(crs, inplace=True)
    # Set the spatial coordinates
    ds.rio.set_spatial_dims(x_dim="x", y_dim="y", inplace=True)
    # Set the spatial attributes
    ds.rio.write_transform(transform, inplace=True)

    return ds.sortby("time")

if __name__ == "__main__":
    logging.basicConfig(filename="preprocess.log", level=logging.INFO)
    
    parser = argparse.ArgumentParser(description="Preprocessing Script")
    parser.add_argument("tile_id", type=str, help="MODIS/VIIRS Tile ID (ex. h11v02)")
    args = parser.parse_args()
    tile_id = args.tile_id
    
    geotiffs = list_input_files(INPUT_DIR)
    geotiff_di = construct_file_dict(geotiffs)
    
    create_single_tile_dataset(geotiff_di, tile_id)
    
    print("Preprocessing Script Complete.")
