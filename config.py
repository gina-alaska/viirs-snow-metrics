"""Configuration for VIIRS Snow Metric Computation"""

import os
from pathlib import Path

from luts import parameter_sets


# path-based required env vars will throw error if None
# path to flat directory of input data downloaded from NSIDC
INPUT_DIR = Path(os.getenv("INPUT_DIR"))
INPUT_DIR.mkdir(exist_ok=True, parents=True)
# path to a working directory for intermediate file I/O
SCRATCH_DIR = Path(os.getenv("SCRATCH_DIR"))
SCRATCH_DIR.mkdir(exist_ok=True, parents=True)
# subdirectory for preprocessed (mosaicked, reprojected, etc.) files
preprocessed_dir = Path(os.getenv("SCRATCH_DIR")).joinpath("preprocessed")
preprocessed_dir.mkdir(exist_ok=True)
# path to a directory for all output files
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR"))
OUTPUT_DIR.mkdir(exist_ok=True)
# subdirectory for metrics files
metrics_dir = Path(os.getenv("OUTPUT_DIR")).joinpath("viirs_snow_metrics")
metrics_dir.mkdir(exist_ok=True)

# Set a "snow_year" (August 1 through July 31)
SNOW_YEAR = os.getenv("SNOW_YEAR")

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
    print("Operating in development mode with the following parameters:")
    print(viirs_params)
