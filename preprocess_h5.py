import os
import logging
import argparse

from shared_utils import list_input_files, write_single_tile_xrdataset
from h5_utils import construct_file_dict_h5, create_single_tile_dataset_from_h5

from config import snow_year_input_dir
from luts import data_variables

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
