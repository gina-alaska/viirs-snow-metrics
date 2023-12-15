# VIIRS Snow Metrics for Alaska

## Development Setup
Create the conda environment from the `environment.yml` file if you have not done so already.
```sh
conda env create -f environment.yml
```
Activate the environment:
```sh
conda activate viirs-snow
```

## System Requirements
Production runs of this code will occur on GINA's "Elephant" machine. Additional testing may occur on SNAP's "Atlas" compute cluster. These are both Linux machines. Other execution environments are not supported. Users will need ample free disk space (order 500 GB) for a snow year.

Developers and users must set the following environment variables:
### Directory Structure
These variables will be read by the configuration file. If the directories do not exist they will be created for you at runtime.
##### `INPUT_DIR`
Set to a path where you will download input data from NSIDC. Anticpate needing around 80 GB free disk space per snow year to download all tiles for the full Alaska spatial domain. Example:
```sh
export INPUT_DIR=/export/datadir/"$USER"_viirs_snow/VIIRS_L3_snow_cover
```
##### `SCRATCH_DIR`
Set to the path where you will read/write preprocessed data prior to computation of the actual metrics. Something like:
```sh
export SCRATCH_DIR=/export/datadir/"$USER"_viirs_snow/scratch
```
##### `OUTPUT_DIR`
Set to a path where you will write the snow metric outputs to disk. Use a shared disk location so multiple users can examine the output data.
```sh
export OUTPUT_DIR=/export/datadir/"$USER"_viirs_snow/VIIRS_snow_metrics
``` 
### Runtime Options
##### `SNOW_YEAR`
Set the "snow year" to download and process. Snow year 2015 begins August 1, 2015 and ends on July 31, 2016. Leap days are included.
```sh
export SNOW_YEAR=2015
``` 
##### `DEV_MODE` (optional)
Set to True (note this will be passed as a `string` type rather than `bool`) to work on a smaller spatial subset for the purpose of improving development speed. Default is True. Setting to False will trigger a production run over the entire Alaska domain.
```sh
export DEV_MODE=True
```

## Usage
### `download.py`
Run this script with no arguments to download the source dataset from the NSIDC DAAC. Users will be prompted to enter valid Earthdata credentials. Users must have an Earthdata account to download the necessary data. Data will be downloaded to the `$INPUT_DIR/$SNOW_YEAR` directory. The script will ask to wipe the contents of `$INPUT_DIR/$SNOW_YEAR` before proceeding with the download. Total download time will of course depend on your connection speed, but it will also depend on how busy the upstream data service is and how complicated your data orders are. For example, if you are asking the service to perform [reformatting (e.g., h5 to GeoTIFF)](https://nsidc.org/data/user-resources/help-center/table-key-value-pair-kvp-operands-subsetting-reformatting-and-reprojection-services) the service will take longer to prepare the order. Consider executing `download.py` in a screen session or similar. It may take a full day to prepare the order for a download of an entire snow year. The order preparation may take longer than the download itself. A log file (`download.log`) that captures the API endpoints used as well as information about the requested granules will be written to the same directory as the download script.
#### Example Usage
`python download.py`

### `preprocess.py`
Run this script with a `tile_id` argument to preprocess the downloaded data. The script will analyze the data from `$INPUT_DIR` and construct a hash table with keys based on the tile of the source dataset and the data variable (one of `"Algorithm_Bit_Flags_QA", "Basic_QA" "CGF_NDSI_Snow_Cover", "Cloud_Persistence", "Daily_NDSI_Snow_Cover"`) represented by the GeoTIFF. The script will construct a time-indexed netCDF file containing the entire set of data for the `SNOW_YEAR` being processed and write the file to `$SCRATCH_DIR/$SNOW_YEAR/preprocessed`. Execution time is about 15 minutes per tile.
#### Example Usage
`python preprocess.py h11v02`

### `compute_masks.py`
Run this script with a `tile_id` argument to create masks from the preproccesed data. This includes water masks with grid cells classified as ocean and as lake or other inland water. Currently the script will create three GeoTIFFs: an ocean mask, a lake / inland water mask, and a combined mask of all grid cells classified as water. Masks will be written to disk as GeoTIFFs to the `$SCRATCH_DIR/$SNOW_YEAR/masks` directory. Execution time is less than 5 minutes per tile. Additional masks may include no data values and areas classified as glaciers or perennial snowfields.
#### Example Usage
`python compute_masks.py h11v02`

### `compute_snow_metrics.py`
Run this script with a `tile_id` argument to compute snow metrics from the preproccesed data. Outputs will be single-band GeoTIFFs (one per metric per tile) written to the `$SCRATCH_DIR/$SNOW_YEAR/single_metric_geotiffs` directory. Execution time is TBD.
#### Example Usage
`python compute_snow_metrics.py h11v02`

## Other Modules
### `shared_utils.py`
This module contains convenience and utility functions that are used across multiple other scripts and modules. At the moment these mostly are functions for file input and output.