"""Configuration for VIIRS Snow Metric Computation. Reads expected environment variables. Required environment variables will throw errors if they are `None`."""

import os
from pathlib import Path

from luts import parameter_sets

# Set "snow_year" (August 1 of SNOW_YEAR through July 31 of SNOW_YEAR + 1)
SNOW_YEAR = os.getenv("SNOW_YEAR")

# path to flat directory of input data downloaded from NSIDC
INPUT_DIR = Path(os.getenv("INPUT_DIR"))
INPUT_DIR.mkdir(exist_ok=True, parents=True)
# subdirectory for input data for a specific snow year
snow_year_input_dir = Path(os.getenv("INPUT_DIR")).joinpath(SNOW_YEAR)
snow_year_input_dir.mkdir(exist_ok=True)

# path to directory for intermediate files
SCRATCH_DIR = Path(os.getenv("SCRATCH_DIR"))
SCRATCH_DIR.mkdir(exist_ok=True, parents=True)
## subdirectory for input data for a specific snow year
snow_year_scratch_dir = Path(os.getenv("SCRATCH_DIR")).joinpath(SNOW_YEAR)
snow_year_scratch_dir.mkdir(exist_ok=True)
### subdirectory for preprocessed datacube
preprocessed_dir = snow_year_scratch_dir.joinpath("preprocessed")
preprocessed_dir.mkdir(exist_ok=True)

### mask subdirectory
mask_dir = snow_year_scratch_dir.joinpath("masks")
mask_dir.mkdir(exist_ok=True)
### subdirectory for reprojected masks
reproj_mask_dir = snow_year_scratch_dir.joinpath("reprojected_mask_geotiffs")
reproj_mask_dir.mkdir(exist_ok=True)
### subdirectory for merged and reprojected masks
reproj_merge_mask_dir = snow_year_scratch_dir.joinpath(
    "reprojected_merged_mask_geotiffs"
)
reproj_merge_mask_dir.mkdir(exist_ok=True)

### single metric GeoTIFF subdirectory
single_metric_dir = snow_year_scratch_dir.joinpath("single_metric_geotiffs")
single_metric_dir.mkdir(exist_ok=True)
### subdirectory for reprojected single metric GeoTIFFs
reproj_single_metric_dir = snow_year_scratch_dir.joinpath(
    "reprojected_single_metric_geotiffs"
)
reproj_single_metric_dir.mkdir(exist_ok=True)
### subdirectory for merged and reprojected single metric GeoTIFFs
reproj_merge_single_metric_dir = snow_year_scratch_dir.joinpath(
    "reprojected_merged_single_metric_geotiffs"
)
reproj_merge_single_metric_dir.mkdir(exist_ok=True)

### uncertainty analysis GeoTIFFs subdirectory
uncertainty_dir = snow_year_scratch_dir.joinpath("uncertainty_geotiffs")
uncertainty_dir.mkdir(exist_ok=True)
### subdirectory for reprojected uncertainty analysis GeoTIFFs
reproj_uncertainty_dir = snow_year_scratch_dir.joinpath(
    "reprojected_uncertainty_geotiffs"
)
reproj_uncertainty_dir.mkdir(exist_ok=True)
### subdirectory for merged and reprojected uncertainty analysis GeoTIFFs
reproj_merge_uncertainty_dir = snow_year_scratch_dir.joinpath(
    "reprojected_merged_uncertainty_geotiffs"
)
reproj_merge_uncertainty_dir.mkdir(exist_ok=True)

# path to a directory for output snow metric results
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR"))
OUTPUT_DIR.mkdir(exist_ok=True)
# subdirectory for metrics files
metrics_dir = Path(os.getenv("OUTPUT_DIR")).joinpath("viirs_snow_metrics")
metrics_dir.mkdir(exist_ok=True)

viirs_params = parameter_sets["prod_params"]

# Nested dict of directories keyed by GeoTIFFs flavor
# top level key is the flavor
# next level key is the type of directory: creation, reprojected, merged
# value is the directory path
tiff_path_dict = {
    "mask": {
        "creation": mask_dir,
        "reprojected": reproj_mask_dir,
        "merged": reproj_merge_mask_dir,
    },
    "single_metric": {
        "creation": single_metric_dir,
        "reprojected": reproj_single_metric_dir,
        "merged": reproj_merge_single_metric_dir,
    },
    "uncertainty": {
        "creation": uncertainty_dir,
        "reprojected": reproj_uncertainty_dir,
        "merged": reproj_merge_uncertainty_dir,
    },
}
