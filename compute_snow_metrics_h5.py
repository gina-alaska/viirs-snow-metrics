"""Compute the VIIRS snow metrics."""

import logging
import argparse
import os

from dask.distributed import Client

from config import (
    preprocessed_dir,
    mask_dir,
    single_metric_dir,
    SNOW_YEAR,
)

from shared_utils import (
    open_preprocessed_dataset,
)
from compute_snow_metrics import process_snow_metrics
from h5_utils import write_tagged_geotiff_from_data_array

if __name__ == "__main__":
    log_file_path = os.path.join(os.path.expanduser("~"), "snow_metric_computation.log")
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', filename=log_file_path, level=logging.INFO)
    parser = argparse.ArgumentParser(description="Snow Metric Computation Script")
    parser.add_argument("tile_id", type=str, help="VIIRS Tile ID (ex. h11v02)")
    parser.add_argument(
        "--alt_input",
        type=str,
        help="Alternate input file indicated by filename suffix.",
    )
    args = parser.parse_args()
    tile_id = args.tile_id
    logging.info(f"Computing snow metrics for tile {tile_id}.")
    # A Dask LocalCluster speeds this script up 10X
    client = Client()
    print("Monitor the Dask client dashboard for progress at the link below:")
    print(client.dashboard_link)
    if args.alt_input is not None:
        alt_input = args.alt_input
        logging.info(f"Using alternate input file: {alt_input}")
        fp = preprocessed_dir / f"snow_year_{SNOW_YEAR}_{tile_id}_{alt_input}.nc"
        chunky_ds = open_preprocessed_dataset(
            fp, {"x": "auto", "y": "auto"}, "CGF_NDSI_Snow_Cover"
        )
        output_tag = alt_input
    else:
        fp = preprocessed_dir / f"snow_year_{SNOW_YEAR}_{tile_id}_filtered_filled.nc"
        chunky_ds = open_preprocessed_dataset(
            fp, {"x": "auto", "y": "auto"}, "CGF_NDSI_Snow_Cover", decode_coords="all"
        )

    logging.info(f"Applying Snow Cover Threshold...")
    
    combined_mask = mask_dir / f"{tile_id}_mask_combined_{SNOW_YEAR}.tif"
    print(chunky_ds.shape)

    
    snow_metrics = process_snow_metrics(chunky_ds, combined_mask)


    chunky_ds.close()

    for metric_name, metric_array in snow_metrics.items():
        metric_array.name = metric_name
        metric_array.rio.set_nodata(0, inplace=True)
        write_tagged_geotiff_from_data_array(
            single_metric_dir,
            tile_id,
            "",
            metric_name,
            SNOW_YEAR,
            metric_array,
            dtype="int16"
        )
        metric_array.close()

    client.close()

    print("Snow Metric Computation Script Complete.")


