"""Utility functions used across multiple modules."""

import logging
import pickle
from pathlib import Path
from datetime import datetime, timedelta

import xarray as xr
import rasterio as rio
import dask.array as da
import numpy as np
from affine import Affine
import h5py
import pyproj

from luts import snow_cover_threshold
from config import SNOW_YEAR, preprocessed_dir, snow_year_scratch_dir


def list_input_files(src_dir, extension="*.tif"):
    """List all .tif files in the source directory.

    Args:
       src_dir (Path): The source directory containing the .tif files.

    Returns:
       list: A list of all .tif files in the source directory.
    """
    fps = [x for x in src_dir.glob(extension)]
    logging.info(f"Downloaded file count is {len(fps)}.")
    return fps


def convert_yyyydoy_to_date(doy_str):
    """Convert a YYYY-DOY string to a datetime object that can be used as an xr.DataSet time index.

    Args:
       doy_str (str): The date in YYYY-DOY format.

    Returns:
       date: date object (e.g., datetime.date(2019, 12, 4))
    """
    year, doy = int(doy_str[0:4]), int(doy_str[-3:])
    date = datetime(year, 1, 1) + timedelta(days=doy - 1)
    return date.date()


def parse_tile(fp):
    """Parse the VIIRS tile ID from the filename.

    Args:
       fp (Path): The file path object.

    Returns:
       str: The tile ID (e.g., h11v02) extracted from the filename.
    """
    return fp.name.split("_")[2]


def open_preprocessed_dataset(fp, chunk_dict, data_variable=None, **kwargs):
    """Open a preprocessed dataset for a given tile.

    Args:
        fp (Path): Path to xarray DataSet
        chunk_dict (dict): how to chunk the dataset, like `{"time": 52}`

    Returns:
       xr.Dataset: The chunked dataset.
    """
    logging.info(f"Opening preprocessed file {fp} as chunked Dataset...")
    if data_variable is not None:
        with xr.open_dataset(fp, **kwargs)[data_variable].chunk(
            chunk_dict
        ) as ds_chunked:
            return ds_chunked
    else:
        with xr.open_dataset(fp, **kwargs).chunk(chunk_dict) as ds_chunked:
            return ds_chunked


def write_single_tile_xrdataset(ds, tile, suffix=None):
    """Write the DataSet to a netCDF file.

    Args:
       ds (xr.Dataset): The single-tile dataset.
       tile (str): The tile being processed.
       suffix (str): An optional suffix to append to the filename.
    """
    if suffix is not None:
        filename = preprocessed_dir / f"snow_year_{SNOW_YEAR}_{tile}_{suffix}.nc"
    else:
        filename = preprocessed_dir / f"snow_year_{SNOW_YEAR}_{tile}.nc"
    ds.to_netcdf(
        filename
    )  # , engine="netcdf4") ## Working without this - should choose engine on it's own
    logging.info(f"NetCDF dataset for tile {tile} wriiten to {filename}.")


def apply_threshold(chunked_cgf_snow_cover):
    """Apply the snow cover threshold to the CGF snow cover datacube. Grid cells exceeding the threshold value are considered to be snow-covered.

    Note that 100 is the maximum valid snow cover value.

    Args:
        chunked_cgf_snow_cover (xr.DataArray): preprocessed CGF snow cover datacube

    Returns:
        snow_on (xr.DataArray): boolean values representing snow cover"""
    snow_on = (chunked_cgf_snow_cover > snow_cover_threshold) & (
        chunked_cgf_snow_cover <= 100
    )
    return snow_on


def fetch_raster_profile(tile_id, updates=None, format="h5", ):
    """Fetch a raster profile to generate output mask rasters that match the downloaded NSIDC rasters.

    We load the GeoTIFF hash table to quicly extract a reference raster creation profile. Preserving these profiles should make the final alignment /
    mosaicking of the raster products a smoother process. We can also use this hash table to perform intermittent QC checks. For example, say FSD = 100 for some grid cell. We should then be able to map that value (100) to a date, then check the GeoTIFFs for that date, the date prior, and the date after, and observe the expected behavior (snow condition toggling from off to on).

    Args:
        tile_id (str): The tile identifier.
        format (str): The file format of the source data, either 'h5' or 'tif'.
        updates (dict): Modifications to the intial raster creation profile e.g., `{"dtype": "int8", "nodata": 0}`
    Returns:
        dict: The raster profile.
    """
    if format == "tif":
        with open(snow_year_scratch_dir / "file_dict.pickle", "rb") as handle:
            geotiff_dict = pickle.load(handle)
        geotiff_reference = geotiff_dict[tile_id]["CGF_NDSI_Snow_Cover"][0]
        with rio.open(geotiff_reference) as src:
            out_profile = src.profile
    else:
        with open(snow_year_scratch_dir / "file_dict_h5.pickle", "rb") as handle:
            h5_dict = pickle.load(handle)
        reference_h5 = h5_dict[tile_id][0]
        x_dim, y_dim = extract_coords_from_viirs_snow_h5(reference_h5)
        crs = create_proj_from_viirs_snow_h5(get_attrs_from_h5(reference_h5))
        transform = initialize_transform_h5(x_dim, y_dim)
        out_profile = {
            "driver": "GTiff",
            "dtype": "uint8",
            "nodata": 0,
            "width": len(x_dim),
            "height": len(y_dim),
            "count": 1,
            "crs": crs,
            "transform": transform,
            "blockxsize": len(x_dim),
            "blockysize": 1,
            "tiled": False,
            "compress": "deflate",
            "interleave": "band",
        }
    if updates is not None:
        out_profile.update(updates)
    logging.info(f"GeoTIFFs will use the raster creation profile {out_profile}.")
    return out_profile

def apply_mask(mask_fp, array_to_mask):
    """Mask out values from an array.

    Args:
        mask_fp (str): file path to the mask GeoTIFF
        array_to_mask (xr.DataArray): array to be masked
    Returns:
        xr.DataArray: masked array where masked values are set to 0
    """

    with rio.open(mask_fp) as src:
        mask_arr = src.read(1)
    mask_applied = mask_arr * array_to_mask
    return mask_applied


def write_tagged_geotiff(dst_dir, tile_id, tag_name, tag_value, out_profile, arr):
    """Write data to a GeoTIFF file.

    Not for multiband or multi-tile GeoTIFFs. Use for masks, single metrics, and other intermediate data products.

    Args:
        dst_dir (Path): Output directory for the GeoTIFF
        tile_id (str): The tile identifier.
        tag_name (str): The name of the metadata tag.
        tag_value (str): Value of the metadata tag.
        out_profile (dict): The raster profile.
        arr (numpy.ndarray): The mask array.

    Returns:
        None
    """
    out_fp = dst_dir / f"{tile_id}_{tag_name}_{tag_value}_{SNOW_YEAR}.tif"
    logging.info(f"Writing GeoTIFF to {out_fp}.")
    with rio.open(out_fp, "w", **out_profile) as dst:
        dst.update_tags(tag_name=tag_value)
        dst.write(arr, 1)
    return None


def parse_date_h5(fp: Path) -> str:
    """Parse the date from an h5 filename.
    Args:
       fp (Path): The file path object.

    Returns:
       str: The date (DOY format) extracted from the filename.
    """
    return fp.name.split(".")[1][1:]


def parse_tile_h5(fp: Path) -> str:
    """Parse the tile ID from an h5 filename.
    Args:
       fp (Path): The file path object.

    Returns:
       str: The tile ID (i.e. 'h11v02') extracted from the filename.
    """
    return fp.name.split(".")[2]


def construct_file_dict_h5(fps: list) -> dict:
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
    with open(snow_year_scratch_dir / "file_dict_h5.pickle", "wb") as handle:
        pickle.dump(di, handle, protocol=pickle.HIGHEST_PROTOCOL)
    return di


def extract_coords_from_viirs_snow_h5(hdf5_path: Path) -> tuple:
    """Extract data arrays of coordinates from a VIIRS snow h5 dataset.

    Args:
        hdf5_path (Path): File path to h5 file.

    Returns:
        (tuple): x and y coordinate data arrays.
    """
    with xr.open_dataset(
        hdf5_path, engine="h5netcdf", group=r"/HDFEOS/GRIDS/VIIRS_Grid_IMG_2D"
    ) as coords:
        return coords["XDim"], coords["YDim"]


def initialize_transform_h5(x_dim, y_dim):
    pixel_size_x = float(abs(x_dim[1] - x_dim[0]))
    pixel_size_y = float(y_dim[1] - y_dim[0])

    origin_x = float(x_dim[0] - (pixel_size_x / 2))
    origin_y = float(y_dim[0] + (pixel_size_y / 2))

    transform = Affine(pixel_size_x, 0, origin_x, 0, pixel_size_y, origin_y)
    return transform


def get_attrs_from_h5(
    dataset_path, dataset_name=r"/HDFEOS/GRIDS/VIIRS_Grid_IMG_2D/Data Fields/Projection"
):
    """Retrieve attributes from a specified dataset in an HDF5 file.

    Args:
        dataset_path (str): Path to the HDF5 file.
        dataset_name (str): Path to the dataset within the HDF5 file.

    Returns:
        (dict): A dictionary of attributes from the dataset.
    """
    with h5py.File(dataset_path, "r") as h5_file:
        if dataset_name not in h5_file:
            raise KeyError(
                f"Dataset '{dataset_name}' not found in the file '{dataset_path}'"
            )

        dataset = h5_file[dataset_name]
        return {
            key: (
                value.item()
                if isinstance(value, np.ndarray) and value.size == 1
                else (
                    value.tolist()
                    if isinstance(value, np.ndarray)
                    else (
                        value.decode("utf-8")
                        if isinstance(value, (np.bytes_, bytes))
                        else value
                    )
                )
            )
            for key, value in dataset.attrs.items()
        }


def create_proj_from_viirs_snow_h5(spatial_metadata: dict) -> pyproj.CRS:
    """Create a coordinate reference system (CRS) from VIIRS snow dataset metadata.

    Args:
        spatial_metadata (dict): Dictionary containing spatial metadata keys extracted from the 'Projection' dataset of a VIIRS snow h5.

    Returns:
        (pyproj.CRS): A coordinate reference system object created from the spatial metadata.
    """
    proj_string = (
        f"+proj={spatial_metadata['grid_mapping_name'][:4]} "
        f"+R={spatial_metadata['earth_radius']} "
        f"+lon_0={spatial_metadata['longitude_of_central_meridian']} "
        f"+x_0={spatial_metadata['false_easting']} "
        f"+y_0={spatial_metadata['false_northing']}"
    )
    return pyproj.CRS.from_proj4(proj_string)


def get_data_array_from_h5(file_path: Path, dataset_name: str, lazy=False) -> da.Array:
    """Extracts the data array from a specified dataset in an HDF5 file.

    Args:
        file_path (Path): Path to the HDF5 file.
        dataset_name (str): Path to the dataset within the file.

    Returns:
        (da.Array): The dask data array from the dataset.
    """
    with h5py.File(file_path, "r") as h5_file:
        if dataset_name not in h5_file:
            raise KeyError(
                f"Dataset '{dataset_name}' not found in the file '{file_path}'"
            )
        if lazy:
            return da.from_array(h5_file[dataset_name])
        else:
            return h5_file[dataset_name][:]


def create_xarray_from_viirs_snow_h5(hdf5_path: Path) -> xr.Dataset:
    """Create an xarray Dataset from a VIIRS snow .h5 file.

    Args:
        hdf5_path (Path): File path to the .h5 file.

    Returns:
        (xr.Dataset): An xarray dataset with coordinates assigned and projection dropped.
    """
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


def make_sorted_h5_stack(
    files: list, yyyydoy_strings: list, variable_path: str, lazy=False
) -> list:
    """Create an in-memory raster stack sorted by date.

    This function takes a list of file paths and a list of chronological (pre-sorted)dates in YYYY-DOY format. It first creates a list of files that match the dates in the list. Then, it opens each of these files, reads the raster data from them, and appends it to the raster stack.

    Args:
       files (list): list of file paths.
       yyyydoy_strings (list): chronologically sorted dates in YYYY-DOY format.
       variable_path (str): The path to the specific variable (i.e. r"/HDFEOS/GRIDS/VIIRS_Grid_IMG_2D/Data Fields/CGF_NDSI_Snow_Cover")

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
        h5_stack.append(get_data_array_from_h5(file, variable_path, lazy))
    return h5_stack


def convert_data_array_to_geotiff(data_array, output_path, **kwargs):
    print(f"Exporting {data_array.name} as {output_path.name}...")
    if not data_array.rio.crs:
        print("Warning: No CRS in data array")
    output_params = {
        "driver": "GTiff",
        "compress": "LZW",
        "tiled": True,
        "dtype": "uint8",
    }
    output_params.update(kwargs)
    data_array.rio.to_raster(output_path, **output_params)
    print("Export complete.")


def write_tagged_geotiff_from_data_array(
    dst_dir, tile_id, tag_name, tag_value, data_array, **kwargs
):
    """Write data to a GeoTIFF file.

    Not for multiband or multi-tile GeoTIFFs. Use for masks, single metrics, and other intermediate data products.

    Args:
        dst_dir (Path): Output directory for the GeoTIFF
        tile_id (str): The tile identifier.
        tag_name (str): The name of the metadata tag.
        tag_value (str): Value of the metadata tag.
        data_array (xarray.core.dataarray.DataArray): The mask data array.
        **kwargs: Options passed to rio.to_raster()

    Returns:
        None
    """

    out_fp = dst_dir / f"{tile_id}_{tag_name}_{tag_value}_{SNOW_YEAR}.tif"

    convert_data_array_to_geotiff(data_array, out_fp, **kwargs)
    return None
