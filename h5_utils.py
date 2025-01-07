from pathlib import Path
import pickle
import xarray as xr
import pyproj
import h5py
import numpy as np

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

def get_attrs_from_h5(dataset_path, dataset_name):
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

def create_single_tile_dataset_forom_h5(tile_di, tile):
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

