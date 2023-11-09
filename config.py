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
# # subdirectory for metrics files
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
    print("Operating in production mode.")
    params = parameter_sets["prod_params"]

else:
    print("Operating in development mode.")
    params = parameter_sets["dev_params"]

print(params)

