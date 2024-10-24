# VIIRS snow metrics post-processing: reproject, mosaic, stack.
import os
import subprocess
import logging
from collections import defaultdict

from config import (
    tiff_path_dict,
    SNOW_YEAR,
)


def reproject_to_3338(target_dir, dst_dir):
    """Reproject all GeoTIFF files in a target directory to EPSG:3338.

    Spawns a `gdalwarp` subprocess with these parameters:
    `gdalwarp -t_srs EPSG:3338 -r nearest -tr 375 375 src.tif dst.tif`

    Args:
        target_dir (str): Path to the directory containing the reprojected GeoTIFF files.
        dst_dir (str): Path to the directory to save the reprojected GeoTIFF files.

    Returns: None
    """

    for file_name in os.listdir(target_dir):
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
                    os.path.join(target_dir, file_name),
                    os.path.join(dst_dir, output),
                ],
                capture_output=True,
                text=True,
            )
            logging.info(log_text.stdout)
            logging.error(log_text.stderr)


def group_files_by_metric(target_dir):
    """Group files in a target directory by metric or variable to prepare them for mosaicking.

    Returns a dictionary with metric or variable names as keys and lists of file paths as values.

    Args:
        target_dir (str): Path to the directory containing the reprojected GeoTIFF files.

    Returns:
        dict: {metric: [file1, file2, ...]}
    """

    geotiff_groups = defaultdict(list)

    for filename in os.listdir(target_dir):
        if filename.endswith("3338.tif"):
            # consider parse_metric function in shared utils
            # tag_to_group = "".join(filename.split("__")[1].split("_")[0:-2])
            tag_to_group = filename.split("__")[1].rsplit("_", 2)[0]
            # above line likely to fail for uncertainty or mask files
            # perhaps the parsing function should be provided as a function to this argument
            geotiff_groups[tag_to_group].append(os.path.join(target_dir, filename))

    return geotiff_groups


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

    # gdalbuildvrt - this is an intermediate file we will throw away
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
    log_file_path = os.path.join(os.path.expanduser("~"), "postprocess.log")
    logging.basicConfig(filename=log_file_path, level=logging.INFO)

    for tiff_flavor in tiff_path_dict.keys():
        logging.info(f"Reprojecting {tiff_flavor} to EPSG:3338...")
        reproject_to_3338(
            tiff_path_dict[tiff_flavor]["creation"],
            tiff_path_dict[tiff_flavor]["reprojected"],
        )
        logging.info("Reprojection complete.")

        file_groups = group_files_by_metric(tiff_path_dict[tiff_flavor]["reprojected"])
        for tag, file_list in file_groups.items():
            logging.info(f"Mosaicing {tiff_flavor} {tag}...")
            dst = (
                tiff_path_dict[tiff_flavor]["merged"] / f"{tag}_merged_{SNOW_YEAR}.tif"
            )
            merge_geotiffs(file_list, dst)
            logging.info(f"Mosaicing {tiff_flavor} {tag} complete.")

    logging.info("Postprocessing complete.")
