"""This script is used to run the snow metrics for a single snow year for all available tiles."""

import os

from config import snow_year_input_dir
from shared_utils import list_input_files, parse_tile


def trigger_download():
    os.system("python download.py")
    print("Download complete.")


def get_unique_tiles_in_input_directory():
    """Get the unique tiles in the input directory.

    Returns:
        list: A list of unique tiles in the input directory.
    """
    fps = list_input_files(snow_year_input_dir)
    tiles_to_process = set([parse_tile(fp) for fp in fps])
    return tiles_to_process


def trigger_preprocess(tile_id):
    os.system("python preprocess.py --tile_id " + tile_id)
    print("Preprocessing complete.")


def trigger_filter_fill(tile_id):
    os.system("python filter_fill.py --tile_id " + tile_id)
    print("Filter and fill complete.")


def trigger_compute_masks(tile_id):
    os.system("python compute_masks.py --tile_id " + tile_id)
    print("Mask computation complete.")


def trigger_compute_snow_metrics(tile_id):
    os.system("python compute_snow_metrics.py --tile_id " + tile_id)
    print("Snow metrics computation complete.")


def trigger_postprocess():
    os.system("python postprocess.py")
    print("Postprocessing complete.")


if __name__ == "__main__":
    # download data by calling the download.py script
    trigger_download()

    tile_ids = get_unique_tiles_in_input_directory()
    for tile_id in tile_ids:
        # preprocess data
        trigger_preprocess(tile_id)

        # filter and fill data
        trigger_filter_fill(tile_id)

        # compute masks
        trigger_compute_masks(tile_id)

        # compute snow metrics
        trigger_compute_snow_metrics(tile_id)

    # postprocess all tiles
    trigger_postprocess()
