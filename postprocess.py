# VIIRS snow metrics post-processing: reproject, mosaic, stack.
import os
import subprocess
import logging
from collections import defaultdict

from config import single_metric_dir, SNOW_YEAR


def reproject_to_3338():
    """Reproject all GeoTIFF files in the single_metric_dir to EPSG:3338.

    Spawns a `gdalwarp` subprocess with these parameters:
    `gdalwarp -t_srs EPSG:3338 -r nearest -tr 375 375 src.tif dst.tif`

    Args: None

    Returns: None
    """

    for file_name in os.listdir(single_metric_dir):
        if file_name.endswith(".tif"):
            base = os.path.basename(file_name)
            name, _ = os.path.splitext(base)

            output = f"{name}_3338.tif"
            # gdalwarp it
            log_text = subprocess.run(
                [
                    "gdalwarp",
                    "-overwrite",
                    "-tap",
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
                ],
                capture_output=True,
                text=True,
            )
            logging.info(log_text.stdout)
            logging.error(log_text.stderr)


def group_files_by_metric():
    """Group files in the single_metric_dir by metric.

    Returns a dictionary with metric names as keys and lists of file paths as values.

    Args: None

    Returns: dict: {metric: [file1, file2, ...]}
    """

    metric_groups = defaultdict(list)

    for filename in os.listdir(single_metric_dir):
        if filename.endswith("3338.tif"):
            # consider parse_metric function in shared utils
            metric = "".join(filename.split("__")[1].split("_")[0:-2])
            metric_groups[metric].append(os.path.join(single_metric_dir, filename))

    return metric_groups


def merge_geotiffs(file_list, output_file):
    """Merge a list of GeoTIFF files into a single GeoTIFF file.

    Spawns a `gdalbuildvrt` subprocess to create a VRT file from the list of files, then a `gdal_translate` subprocess to convert the VRT to a GeoTIFF.

    Args:
        file_list (list): List of file paths to merge.
        output_file (str): Path to the output GeoTIFF file.

    Returns: None
    """
    # creating a temp textfile to pipe into gdalbuildvrt
    merge_list_file = "list_of_files_to_merge.txt"
    with open(merge_list_file, "w") as f:
        f.write("\n".join(file_list))

    # gdalbuildvrt it
    vrt_file = "output.vrt"
    log_text = subprocess.run(
        [
            "gdalbuildvrt",
            "-input_file_list",
            merge_list_file,
            "-resolution",
            "highest",
            "-r",
            "nearest",
            vrt_file,
        ],
        capture_output=True,
        text=True,
    )
    logging.info(log_text.stdout)
    logging.error(log_text.stderr)

    # gdal_translate converts the VRT to a GeoTIFF
    log_text = subprocess.run(
        [
            "gdal_translate",
            "-of",
            "GTiff",
            "-co",
            "COMPRESS=DEFLATE",
            "-co",
            "NUM_THREADS=ALL_CPUS",
            vrt_file,
            output_file,
        ],
        capture_output=True,
        text=True,
    )
    logging.info(log_text.stdout)
    logging.error(log_text.stderr)
    # cull the temp files
    os.remove(merge_list_file)
    os.remove(vrt_file)


if __name__ == "__main__":
    logging.basicConfig(filename="postprocess.log", level=logging.INFO)

    logging.info("Reprojecting to EPSG:3338...")
    reproject_to_3338()
    logging.info("Reprojection complete.")
    file_groups = group_files_by_metric()
    for metric, file_list in file_groups.items():
        logging.info(f"Mosaicing {metric}...")
        dst = single_metric_dir / f"{metric}_merged_{SNOW_YEAR}.tif"
        merge_geotiffs(file_list, dst)
        logging.info(f"Mosaicing {metric} complete.")

    logging.info("Postprocessing complete.")
