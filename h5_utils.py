from pathlib import Path
import pickle
import xarray as xr
import pyproj
import h5py
import numpy as np
import pandas as pd
from affine import Affine
import dask.array as da

from shared_utils import convert_yyyydoy_to_date
from luts import data_variables

def parse_date_h5(fp: Path) -> str:
    """Parse the date from an h5 filename.
    Args:
       fp (Path): The file path object.

    Returns:
       str: The date (DOY format) extracted from the filename.
    """
    return fp.name.split(".")[1][1:]

def parse_tile_h5(fp: Path) -> str:
    return fp.name.split(".")[2]

def construct_file_dict_h5(fps):
    """Construct a dict mapping tiles and data variables to file paths.

    Args:
       fps (list): A list of file paths.

    Returns:
       (dict): hierarchical dict with keys of tile>>>data_variable that map to the file paths of the downloaded GeoTIFFs.
    """
    di = dict()
    for fp in fps:
        tile = parse_tile_h5(fp)
        if tile not in di:
            di[tile] = []
        di[tile].append(fp)
    # persist the dict as pickle for later reference
    with open("file_dict_h5.pickle", "wb") as handle:
        pickle.dump(di, handle, protocol=pickle.HIGHEST_PROTOCOL)
    return di

def extract_coords_from_viirs_snow_h5(hdf5_path):
    with xr.open_dataset(
        hdf5_path, engine="h5netcdf", group=r"/HDFEOS/GRIDS/VIIRS_Grid_IMG_2D"
    ) as coords:
        return coords["XDim"], coords["YDim"]

def initialize_transform_h5(x_dim, y_dim):
    pixel_size_x = abs(x_dim[1] - x_dim[0])
    pixel_size_y = abs(y_dim[1] - y_dim[0])

    origin_x = x_dim[0] - (pixel_size_x / 2)
    origin_y = y_dim[0] + (pixel_size_y / 2)

    transform = Affine(pixel_size_x, 0, origin_x, 0, -pixel_size_y, origin_y)
    return transform

def get_attrs_from_h5(dataset_path, dataset_name=r"/HDFEOS/GRIDS/VIIRS_Grid_IMG_2D/Data Fields/Projection"):
    """Retrieve attributes from a specified dataset in an HDF5 file.

    Args:
        dataset_path (str): Path to the HDF5 file.
        dataset_name (str): Path to the dataset within the HDF5 file.

    Returns:
        dict: A dictionary of attributes from the dataset.
    """
    with h5py.File(dataset_path, 'r') as h5_file:
        if dataset_name not in h5_file:
            raise KeyError(f"Dataset '{dataset_name}' not found in the file '{dataset_path}'")
        
        dataset = h5_file[dataset_name]
        return {
            key: (
                value.item() if isinstance(value, np.ndarray) and value.size == 1
                else value.tolist() if isinstance(value, np.ndarray)
                else value.decode('utf-8') if isinstance(value, (np.bytes_, bytes))
                else value
            )
            for key, value in dataset.attrs.items()
        }

def create_proj_from_viirs_snow_h5(spatial_metadata):
    proj_string = (
        f"+proj={spatial_metadata['grid_mapping_name'][:4]} "
        f"+R={spatial_metadata['earth_radius']} "
        f"+lon_0={spatial_metadata['longitude_of_central_meridian']} "
        f"+x_0={spatial_metadata['false_easting']} "
        f"+y_0={spatial_metadata['false_northing']}"
    )
    return pyproj.CRS.from_proj4(proj_string)

def get_data_array_from_h5(file_path, dataset_name):
    """Extracts the data array from a specified dataset in an HDF5 file.

    Args:
        file_path (str): Path to the HDF5 file.
        dataset_name (str): Path to the dataset within the file.

    Returns:
        np.ndarray: The data array from the dataset.
    """
    with h5py.File(file_path, 'r') as h5_file:
        if dataset_name not in h5_file:
            raise KeyError(f"Dataset '{dataset_name}' not found in the file '{file_path}'")
        return da.array(h5_file[dataset_name][:])

def create_xarray_from_viirs_snow_h5(hdf5_path):
    dataset = xr.open_dataset(
        hdf5_path,
        engine="h5netcdf",
        group=r"/HDFEOS/GRIDS/VIIRS_Grid_IMG_2D/Data Fields",
        phony_dims="access",
    )
    x_dim, y_dim = extract_coords_from_viirs_snow_h5(hdf5_path)
    dataset = dataset.assign_coords(XDim=x_dim, YDim=y_dim)
    dataset = dataset.drop_vars("Projection", errors="ignore")

    return dataset

def make_sorted_h5_stack(files, yyyydoy_strings, variable_path):
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
            if yyyydoy == parse_date_h5(f):
                sorted_files.append(f)

    h5_stack = []
    for file in sorted_files:
        h5_stack.append(get_data_array_from_h5(file, variable_path))
    return h5_stack

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

    dates = [
        convert_yyyydoy_to_date(parse_date_h5(x))
        for x in tile_di[tile]
    ]
    dates.sort()
    yyyydoy_strings = [d.strftime("%Y") + d.strftime("%j") for d in dates]


    ds_dict = dict()
    ds_coords = {
        "time": pd.DatetimeIndex(dates),
        "x": x_dim.values,
        "y": y_dim.values, 
    }
    
    for data_var in data_variables:
        #logging.info(f"Stacking data for {data_var}...")
        variable_path=rf"/HDFEOS/GRIDS/VIIRS_Grid_IMG_2D/Data Fields/{data_var}"
        raster_stack = make_sorted_h5_stack(
            tile_di[tile], yyyydoy_strings, variable_path
        )
        data_var_dict = {data_var: (["time", "y", "x"], da.array(raster_stack))}
        ds_dict.update(data_var_dict)

    #datasets = []
    #for h5_path in tile_di[tile]:
    #    dt = convert_yyyydoy_to_date(parse_date_h5(h5_path))

    #    dataset = create_xarray_from_viirs_snow_h5(h5_path)

    #    dataset = dataset.expand_dims({'time': [dt]})
        
    #    datasets.append(dataset)

    ds = xr.Dataset(ds_dict, coords=ds_coords)
    
    #ds = xr.concat(datasets, dim='time')
    ds.rio.write_crs(crs, inplace=True)
    ds.rio.set_spatial_dims(x_dim="x", y_dim="y", inplace=True)
    ds.rio.write_transform(transform, inplace=True)
    return ds
