import argparse
import os
import re
import xarray as xr
import rioxarray as rio
import h5py
import numpy as np
from pathlib import Path
import pyproj


def parse_metadata(hdf):
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


def extract_coords_from_viirs_snow_h5(hdf5_path):
    with xr.open_dataset(
        hdf5_path, engine="h5netcdf", group=r"/HDFEOS/GRIDS/VIIRS_Grid_IMG_2D"
    ) as coords:
        return coords["XDim"], coords["YDim"]


def create_proj_from_viirs_snow_h5(spatial_metadata):
    proj_string = (
        f"+proj={spatial_metadata['grid_mapping_name'][:4]} "
        f"+R={spatial_metadata['earth_radius']} "
        f"+lon_0={spatial_metadata['longitude_of_central_meridian']} "
        f"+x_0={spatial_metadata['false_easting']} "
        f"+y_0={spatial_metadata['false_northing']}"
    )
    return pyproj.CRS.from_proj4(proj_string)


def convert_data_array_to_geotiff(data_array, output_path):

    print(f"Converting {data_array} to {output_path}...")


def create_xarray_from_viirs_snow_h5(hdf5_path):
    dataset = xr.open_dataset(
        hdf5_path,
        engine="h5netcdf",
        group=r"/HDFEOS/GRIDS/VIIRS_Grid_IMG_2D/Data Fields",
        phony_dims="access",
    )
    x_dim, y_dim = extract_coords_from_viirs_snow_h5(hdf5_path)
    crs = create_proj_from_viirs_snow_h5(dataset["Projection"].attrs)
    dataset = dataset.drop_vars("Projection", errors="ignore")
    dataset.rio.write_crs(crs.to_wkt())

    return dataset


def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(
        description="Convert HDF5 files to Georeferenced GeoTIFF files."
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
        help="Directory to save the output GeoTIFF files (default: ./geotiff ).",
    )

    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Process each HDF5 file
    for hdf5_file in args.files:
        hdf5_path = Path(hdf5_file)
        if not hdf5_path.exists() or not hdf5_path.is_file():
            print(f"Warning: File not found or not a file: {hdf5_path}")
            continue

        with h5py.File(hdf5_path, "r") as hdf:
            UpperLeftX, UpperLeftY, LowerRightX, LowerRightY = parse_metadata(hdf)

        # Set output GeoTIFF path
        output_path = output_dir / (hdf5_path.stem + ".tif")

        dataset = create_xarray_from_viirs_snow_h5(hdf5_path)

        for da in dataset.data_vars:
            convert_data_array_to_geotiff(hdf5_path, output_path)


if __name__ == "__main__":
    main()
