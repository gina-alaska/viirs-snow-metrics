import xarray as xr
from shared_utils import open_preprocessed_dataset
from luts import inv_cgf_codes
from pathlib import Path
import argparse
import os
import pandas as pd

from config import (
    preprocessed_dir,
    SNOW_YEAR,
)

def main(tile_id, lat, lon, nc_path):
    print("Printing data for:", nc_path)

    snow_ds = open_preprocessed_dataset(
        nc_path, {"x": "auto", "y": "auto"}, "CGF_NDSI_Snow_Cover"
    )
    bitflag_ds = open_preprocessed_dataset(
        nc_path, {"x": "auto", "y": "auto"}, "Algorithm_Bit_Flags_QA"
    )

    snow_values = snow_ds.sel(x=lat, y=lon, method="nearest").values
    bitflag_values = bitflag_ds.sel(x=lat, y=lon, method="nearest").values

    print(snow_values)
    print(bitflag_values)

    print(inv_cgf_codes)

    snow_ds.close()
    bitflag_ds.close()

    if os.path.exists(
        preprocessed_dir / f"snow_year_{SNOW_YEAR}_{tile_id}_filtered_filled.nc"
    ):
        fp = preprocessed_dir / f"snow_year_{SNOW_YEAR}_{tile_id}_filtered_filled.nc"
        print("Printing data for:", fp)

        snow_ds = open_preprocessed_dataset(
            fp, {"x": "auto", "y": "auto"}, "CGF_NDSI_Snow_Cover"
        )

        snow_values_ff = snow_ds.sel(x=lat, y=lon, method="nearest").values

        print(snow_values_ff)

        snow_ds.close()

        df = pd.DataFrame(
            {"bitflag": bitflag_values, "raw_snow": snow_values, "ff_snow": snow_values_ff}
        )
    else:
        df = pd.DataFrame({"bitflag": bitflag_values, "raw_snow": snow_values})

    return df

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Returns single point time series of snow and bitflag data for a given preprocessed dataset"
    )

    parser.add_argument("tile_id", type=str, help="VIIRS Tile ID (ex. h11v02)")
    parser.add_argument(
        "coordinates",
        type=float,
        nargs=2,
        metavar=("lon", "lat"),
        help="Coordinates as longitude and latitude (e.g., -147.3 66.8)",
    )
    parser.add_argument(
        "-nc", "--nc_path", type=Path, default=None, help="optional netcdf file path"
    )
    parser.add_argument(
        "-o",
        "--output_csv",
        type=Path,
        default=None,
        help="Optional output path to save .csv of dataframe",
    )

    args = parser.parse_args()
    print(args)

    tile_id = args.tile_id
    lat, lon = args.coordinates
    nc_path = args.nc_path if args.nc_path else preprocessed_dir / f"snow_year_{SNOW_YEAR}_{tile_id}.nc"

    df = main(tile_id, lat, lon, nc_path)
    print(df)

    if args.output_csv:
        df.to_csv(args.output_csv)
