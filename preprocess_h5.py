import os
import logging
import argparse
import pandas as pd
import xarray as xr
import dask.array as da

from shared_utils import (
    list_input_files,
    write_single_tile_xrdataset,
    convert_yyyydoy_to_date,
)
from h5_utils import (
    construct_file_dict_h5,
    extract_coords_from_viirs_snow_h5,
    initialize_transform_h5,
    create_proj_from_viirs_snow_h5,
    get_attrs_from_h5,
    parse_date_h5,
    make_sorted_h5_stack,
)

from config import snow_year_input_dir
from luts import data_variables


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
    transform = initialize_transform_h5(x_dim, y_dim)
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
        raster_stack = make_sorted_h5_stack(
            tile_di[tile], yyyydoy_strings, variable_path
        )
        data_var_dict = {data_var: (["time", "y", "x"], da.array(raster_stack))}
        ds_dict.update(data_var_dict)

    ds = xr.Dataset(ds_dict, coords=ds_coords)

    ds.rio.write_crs(crs, inplace=True)
    ds.rio.set_spatial_dims(x_dim="x", y_dim="y", inplace=True)
    ds.rio.write_transform(transform, inplace=True)
    return ds


if __name__ == "__main__":
    log_file_path = os.path.join(os.path.expanduser("~"), "datacube_preprocess.log")
    logging.basicConfig(filename=log_file_path, level=logging.INFO)

    parser = argparse.ArgumentParser(description="Preprocessing Script - HDF5")
    parser.add_argument("tile_id", type=str, help="VIIRS Tile ID (ex. h11v02)")
    args = parser.parse_args()
    tile_id = args.tile_id

    logging.info(f"Creating preprocessed dataset for tile {tile_id}...")
    print(snow_year_input_dir)

    h5_paths = list_input_files(snow_year_input_dir, extension="*.h5")
    h5_dict = construct_file_dict_h5(h5_paths)
    print(len(h5_paths))
    tile_ds = create_single_tile_dataset_from_h5(h5_dict, tile_id)
    write_single_tile_xrdataset(tile_ds, tile_id)

    logging.info(f"Creating preprocessed dataset for tile {tile_id} complete.")
    print("Preprocessing Script Complete.")
