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
#   subdirectory for input data for a specific snow year
snow_year_scratch_dir = Path(os.getenv("SCRATCH_DIR")).joinpath(SNOW_YEAR)
snow_year_scratch_dir.mkdir(exist_ok=True)
#     subdirectory for preprocessed datacube
preprocessed_dir = snow_year_scratch_dir.joinpath("preprocessed")
preprocessed_dir.mkdir(exist_ok=True)
#     subdirectory for mask files
mask_dir = snow_year_scratch_dir.joinpath("masks")
mask_dir.mkdir(exist_ok=True)
#     subdirectory for single metric GeoTIFFs
single_metric_dir = snow_year_scratch_dir.joinpath("single_metric_geotiffs")
single_metric_dir.mkdir(exist_ok=True)
#     subdirectory for uncertainty analysis GeoTIFFs
uncertainty_dir = snow_year_scratch_dir.joinpath("uncertainty_geotiffs")
uncertainty_dir.mkdir(exist_ok=True)


# path to a directory for output snow metric results
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR"))
OUTPUT_DIR.mkdir(exist_ok=True)
# subdirectory for metrics files
metrics_dir = Path(os.getenv("OUTPUT_DIR")).joinpath("viirs_snow_metrics")
metrics_dir.mkdir(exist_ok=True)

# Configure development vs. production runtimes, default to dev
if os.getenv("DEV_MODE") is None:
    DEV_MODE = True
elif os.getenv("DEV_MODE").lower() == "false":
    DEV_MODE = False
else:
    DEV_MODE = True

if not DEV_MODE:
    viirs_params = parameter_sets["prod_params"]
    print("Operating in production mode with the following parameters:")
    print(viirs_params)
else:
    viirs_params = parameter_sets["dev_params"]
