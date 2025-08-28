"""Script to run snow metrics for a single snow year for all available tiles."""

import os
import subprocess
import argparse

from config import snow_year_input_dir
from shared_utils import list_input_files, parse_tile, parse_tile_h5
from luts import needed_tile_ids


def trigger_download(format="h5"):
    if format == "h5":
        os.system("python ./download_h5.py")
    else:
        os.system("python ./download.py")
    print("Download complete.")


def get_unique_tiles_in_input_directory(format="h5"):
    """Get the unique tiles in the input directory.

    Returns:
        list: A list of unique tiles in the input directory.
    """
    if format == "h5":
        fps = list_input_files(snow_year_input_dir, extension="*.h5")
        tiles_to_process = set([parse_tile_h5(fp) for fp in fps])
    else:
        fps = list_input_files(snow_year_input_dir)
        tiles_to_process = set([parse_tile(fp) for fp in fps])
    return list(tiles_to_process)


def trigger_preprocess(tile_id, format="h5"):
    script = "./preprocess.py"
    try:
        result = subprocess.check_output(
            ["python", script, tile_id, format], stderr=subprocess.STDOUT
        )
        print(result)
    except subprocess.CalledProcessError as e:
        print("Error occurred:", e.output.decode())
    print("Preprocessing complete.")


def trigger_filter_fill(tile_id):
    try:
        result = subprocess.check_output(
            ["python", "./filter_and_fill.py", tile_id], stderr=subprocess.STDOUT
        )
        print(result)
    except subprocess.CalledProcessError as e:
        print("Error occurred:", e.output.decode())
    print("Filter and fill complete.")


def trigger_compute_masks(tile_id, format="h5"):
    script = "./compute_masks.py"
    try:
        result = subprocess.check_output(
            ["python", script, tile_id, format], stderr=subprocess.STDOUT
        )
        print(result)
    except subprocess.CalledProcessError as e:
        print("Error occurred:", e.output.decode())
    print("Mask computation complete.")


def trigger_compute_snow_metrics(tile_id, format="h5"):
    script = "./compute_snow_metrics.py"
    try:
        result = subprocess.check_output(
            ["python", script, tile_id, format], stderr=subprocess.STDOUT
        )
        print(result)
    except subprocess.CalledProcessError as e:
        print("Error occurred:", e.output.decode())
    print("Snow metrics computation complete.")


def trigger_postprocess():
    try:
        result = subprocess.check_output(
            ["python", "./postprocess.py"], stderr=subprocess.STDOUT
        )
        print(result)
    except subprocess.CalledProcessError as e:
        print("Error occurred:", e.output.decode())
    print("Postprocessing complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run snow metrics for a single snow year for all available tiles."
    )
    parser.add_argument("--download", action="store_true", help="Trigger data download")
    parser.add_argument(
        "--preprocess", action="store_true", help="Trigger data preprocessing"
    )
    parser.add_argument(
        "--filter_fill", action="store_true", help="Trigger filter and fill"
    )
    parser.add_argument(
        "--compute_masks", action="store_true", help="Trigger mask computation"
    )
    parser.add_argument(
        "--compute_metrics",
        action="store_true",
        help="Trigger snow metrics computation",
    )
    parser.add_argument(
        "--postprocess", action="store_true", help="Trigger postprocessing"
    )
    parser.add_argument(
        "--format",
        choices=["tif", "h5"],
        default="h5",
        help="Download/input File format: Older processing run downloads and uses tif, newer downloads and uses h5",
    )

    args = parser.parse_args()
    format = args.format

    if args.download:
        trigger_download(format)

    tile_ids = get_unique_tiles_in_input_directory(format)
    for tile_id in tile_ids:
        if tile_id not in needed_tile_ids:
            continue
        print(tile_id)
        if args.preprocess:
            trigger_preprocess(tile_id, format)

        if args.filter_fill:
            trigger_filter_fill(tile_id)

        if args.compute_masks:
            trigger_compute_masks(tile_id, format)

        if args.compute_metrics:
            trigger_compute_snow_metrics(tile_id, format)

    if args.postprocess:
        trigger_postprocess()

    print("All tasks complete.")
