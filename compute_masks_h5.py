"""Create a mask of grid cells that will be excluded from snow metric computations. This includes ocean and lake / inland water grid cells. These grid cells will be excluded from the snow metric computations. Each snow year will have an independent mask set to account for dynamic coastal and fluvial geomorphology."""

import argparse
import logging
import os

import numpy as np
from dask.distributed import Client

from config import SNOW_YEAR, preprocessed_dir, mask_dir
from luts import n_obs_to_classify_ocean, n_obs_to_classify_inland_water, inv_cgf_codes
from shared_utils import (
    open_preprocessed_dataset,
    fetch_raster_profile,
    write_tagged_geotiff,
)
from compute_masks import (
    generate_inland_water_mask,
    generate_l2fill_mask,
    generate_ocean_mask,
    combine_masks,
)
from h5_utils import write_tagged_geotiff_from_data_array

def process_masks(ds):
    
    ocean_mask = generate_ocean_mask(ds)
    ocean_mask.name = "ocean_mask"
    ocean_mask.rio.set_nodata(0, inplace=True)

    inland_water_mask = generate_inland_water_mask(ds)
    inland_water_mask.name = "inland_water_mask"
    inland_water_mask.rio.set_nodata(0, inplace=True)

    l2_mask = generate_l2fill_mask(ds)
    l2_mask.name = "l2_fill_mask"
    l2_mask.rio.set_nodata(0, inplace=True)

    combined_mask = combine_masks([ocean_mask, inland_water_mask, l2_mask])
    combined_mask.name = "combined_mask"
    combined_mask.rio.set_nodata(0, inplace=True)

    return ocean_mask, inland_water_mask, l2_mask, combined_mask


def main(tile_id, input_dir, output_dir, year):

    client = Client(n_workers=24)
    fp = input_dir / f"snow_year_{year}_{tile_id}.nc"
    ds = open_preprocessed_dataset(
        fp, {"x": "auto", "y": "auto"}, "CGF_NDSI_Snow_Cover"
    )

    ocean_mask, inland_water_mask, l2_mask, combined_mask = process_masks(ds)

    write_tagged_geotiff_from_data_array(
        output_dir, tile_id, "mask", "ocean", year, ocean_mask, nodata=0
    )
    write_tagged_geotiff_from_data_array(
        output_dir, tile_id, "mask", "inland_water", year, inland_water_mask, nodata=0
    )
    write_tagged_geotiff_from_data_array(
        output_dir, tile_id, "mask", "l2_fill", year, l2_mask, nodata=0
    )
    write_tagged_geotiff_from_data_array(
        output_dir, tile_id, "mask", "combined", year, combined_mask, nodata=0
    )

    ds.close()
    client.close()

    return None


if __name__ == "__main__":
    log_file_path = os.path.join(os.path.expanduser("~"), "mask_computation.log")
    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(message)s",
        filename=log_file_path,
        level=logging.INFO,
    )

    parser = argparse.ArgumentParser(description="Script to Generate Masks")
    parser.add_argument("tile_id", type=str, help="VIIRS Tile ID (ex. h11v02)")
    args = parser.parse_args()
    tile_id = args.tile_id
    logging.info(f"Creating masks for tile {tile_id} for snow year {SNOW_YEAR}.")

    main(tile_id, preprocessed_dir, mask_dir, SNOW_YEAR)

    logging.info(f"Mask Generation Script Complete, GeoTIFFs written to {mask_dir}")
