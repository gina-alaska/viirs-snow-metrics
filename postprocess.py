# VIIRS snow metrics post-processing: reproject, mosaic, stack.
import os
import subprocess
import logging
import argparse

from config import single_metric_dir, SNOW_YEAR


def reproject_to_3338():
    """Reproject all GeoTIFF files in the single_metric_dir to EPSG:3338.

    Spawns a `gdalwarp` subprocess with these parameters:
    `gdalwarp -t_srs EPSG:3338 -r nearest -tr 375 375 src.tif dst.tif`
    """

    for file_name in os.listdir(single_metric_dir):
        if file_name.endswith(".tif"):
            base = os.path.basename(file_name)
            name, _ = os.path.splitext(base)
            # Construct the output file name by appending "_3338" to the base name
            output = f"{name}_3338.tif"
            # Apply the gdalwarp command to the file
            subprocess.run(
                [
                    "gdalwarp",
                    "-t_srs",
                    "EPSG:3338",
                    "-r",
                    "nearest",
                    "-tr",
                    "375",
                    "375",
                    "-co",
                    "COMPRESS=DEFLATE",
                    "-co",
                    "NUM_THREADS=ALL_CPUS",
                    os.path.join(single_metric_dir, file_name),
                    os.path.join(single_metric_dir, output),
                ]
            )


if __name__ == "__main__":
    logging.basicConfig(filename="postprocess.log", level=logging.INFO)

    parser = argparse.ArgumentParser(
        description="Script to Postprocess VIIRS Snow Metrics"
    )
    # We can just reproject the entire directory of files, so we don't need to specify a tile ID at the moment
    # parser.add_argument("tile_id", type=str, help="VIIRS Tile ID (ex. h11v02)")
    # args = parser.parse_args()
    # tile_id = args.tile_id
    #logging.info(f"Postprocessing data for tile {tile_id} for snow year {SNOW_YEAR}...")
    reproject_to_3338()
    logging.info("Postprocessing complete.")
