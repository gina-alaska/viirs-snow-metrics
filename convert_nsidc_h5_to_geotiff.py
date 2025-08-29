import argparse
import os
import re
import xarray as xr
import rioxarray as rio
from rasterio.enums import Resampling
import h5py
import numpy as np
from pathlib import Path
import pyproj

from shared_utils import (
    get_attrs_from_h5,
    create_proj_from_viirs_snow_h5,
    convert_data_array_to_geotiff,
    create_xarray_from_viirs_snow_h5,
)


def parse_metadata(hdf):
    """Parse the metadata from the HDF5 file to extract the upper left and lower right coordinates.

    Args:
        hdf (h5py.File): The HDF5 file object.
    Returns:
        tuple: A tuple containing the upper left x, upper left y, lower right x, and lower right y coordinates as strings.
    """
    gridmeta = hdf["HDFEOS INFORMATION"]["StructMetadata.0"][()].decode("ascii")

    ul_regex = re.compile(
        r"""UpperLeftPointMtrs=\(
                                (?P<upper_left_x>[+-]?\d+\.\d+)
                                ,
                                (?P<upper_left_y>[+-]?\d+\.\d+)
                                \)""",
        re.VERBOSE,
    )
    match = ul_regex.search(gridmeta)
    ulx = match.group("upper_left_x")
    uly = match.group("upper_left_y")

    lr_regex = re.compile(
        r"""LowerRightMtrs=\(
                                (?P<lower_right_x>[+-]?\d+\.\d+)
                                ,
                                (?P<lower_right_y>[+-]?\d+\.\d+)
                                \)""",
        re.VERBOSE,
    )
    match = lr_regex.search(gridmeta)
    lrx = match.group("lower_right_x")
    lry = match.group("lower_right_y")
    return ulx, uly, lrx, lry


def project_dataset(data_array, epsg):
    """Reproject a rioxarray object to the specified EPSG code.

    Args:
        data_array (rioxarray.raster_array.RasterArray or xarray.DataArray): The rioxarray object to reproject.
        epsg (int): The EPSG code to reproject to.

    Returns:
        rioxarray.raster_array.RasterArray or xarray.DataArray: The reprojected rioxarray object.
    """
    target_crs = pyproj.CRS.from_epsg(epsg)
    if target_crs.axis_info[0].unit_name in ["metre", "meter"]:
        resolution = 375
    else:
        resolution = None
    data_array = data_array.rio.reproject(
        target_crs, resampling=Resampling.nearest, resolution=resolution
    )
    return data_array


def add_overviews(output_path):
    """Add internal overviews to a GeoTIFF file.
    Args:
        output_path (str or Path): The path to the GeoTIFF file.

    Returns:
        None
    """
    import rasterio

    with rasterio.open(output_path, "r+") as ds:
        ds.build_overviews([2, 4, 8, 16, 32], Resampling.nearest)
        ds.update_tags(ns="rio_overview", resampling="nearest")


def main():
    parser = argparse.ArgumentParser(
        description="""
        Convert any number of HDF5 files from NSIDC datasets to Georeferenced GeoTIFF files.
        Currently designed for use with VNP10A1F and VJ110AF1 data - would need to be reworked for use with others.
        Default output is reprojected to EPSG:4326, can skip reprojection or use other output SRS with the --epsg flag
        """
    )
    parser.add_argument(
        "files",
        metavar="FILE",
        nargs="+",
        help="Paths to the HDF5 files to be converted.",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        metavar="DIR",
        default="./geotiff",
        help="Directory to save the output GeoTIFF files. Default: ./geotiff",
    )
    parser.add_argument(
        "--epsg",
        type=int,
        default=4326,
        help="EPSG code for reprojection. Default: 4326. Enter 0 to skip reprojection.",
    )
    parser.add_argument(
        "--overviews",
        action="store_true",
        help="Generate internal overviews after geotiff creation. Default: False",
    )

    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    epsg = args.epsg
    overviews = args.overviews

    # Process each HDF5 file entered as an argument
    for hdf5_file in args.files:

        hdf5_path = Path(hdf5_file)

        if not hdf5_path.exists() or not hdf5_path.is_file():
            print(f"Warning: File not found or not a file: {hdf5_path}")
            continue

        print("Processing", hdf5_path.name)

        # Unused, may be relavant for other datasets
        with h5py.File(hdf5_path, "r") as hdf:
            UpperLeftX, UpperLeftY, LowerRightX, LowerRightY = parse_metadata(hdf)

        dataset = create_xarray_from_viirs_snow_h5(hdf5_path)

        crs = create_proj_from_viirs_snow_h5(get_attrs_from_h5(hdf5_path))
        for data_var in dataset.data_vars:
            da = dataset[data_var]
            da = da.rio.write_crs(crs)
            if epsg:
                da = project_dataset(da, epsg)
            output_path = output_dir / (".".join([da.name, hdf5_path.stem, "tif"]))
            convert_data_array_to_geotiff(da, output_path, dtype=da.dtype)

            if overviews:
                print("Generating internal overviews")
                add_overviews(output_path)


if __name__ == "__main__":
    main()
