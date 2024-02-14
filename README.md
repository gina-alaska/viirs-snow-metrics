# VIIRS Snow Metrics for Alaska

## System Requirements
Production runs of this code will execute on GINA's "Elephant" machine. Additional testing may occur on SNAP's "Atlas" compute cluster. These are both Linux machines. Other execution environments are not supported. Users will need ample free disk space (order 500 GB) per snow year.

## Setup
Create the conda environment from the `environment.yml` file if you have not done so already.
```sh
conda env create -f environment.yml
```
Activate the environment:
```sh
conda activate viirs-snow
```

### Environment Variables
Developers and users must set the following environment variables:
#### Directory Structure
These variables will be read by the configuration file. If the directories do not exist they will be created for you at runtime.
###### `INPUT_DIR`
Set to a path where you will download and reference the input data from NSIDC. Anticpate needing around 80 GB free disk space per snow year to download all tiles for the full Alaska spatial domain. Example:
```sh
export INPUT_DIR=/export/datadir/"$USER"_viirs_snow/VIIRS_L3_snow_cover
```
###### `SCRATCH_DIR`
Set the path where you will read/write intermediate data. Something like:
```sh
export SCRATCH_DIR=/export/datadir/"$USER"_viirs_snow/scratch
```
The scratch directory structure will look something like this:
```
/export/datadir/$USER_viirs_snow/scratch
└── $SNOW_YEAR
    ├── masks
    ├── preprocessed
    ├── reprojected_merged_single_metric_geotiffs
    ├── reprojected_single_metric_geotiffs
    ├── single_metric_geotiffs
    └── uncertainty_geotiffs
```
###### `OUTPUT_DIR`
Set to a path where you will write the snow metric outputs to disk. Use a shared disk location so multiple users can examine the output data.
```sh
export OUTPUT_DIR=/export/datadir/"$USER"_viirs_snow/VIIRS_snow_metrics
``` 
#### Runtime Options
###### `SNOW_YEAR`
Set the "snow year" to download and process. Snow year 2015 begins August 1, 2015 and ends on July 31, 2016. Leap days are included.
```sh
export SNOW_YEAR=2015
``` 
###### `DEV_MODE` (optional)
Set to True (note this will be passed as a `string` type rather than `bool`) to work on a smaller spatial subset for the purpose of improving development speed. Default is True. Setting to False will trigger a production run over the entire Alaska domain.
```sh
export DEV_MODE=True
```

###### `MALLOC_TRIM_THRESHOLD_` (optional)
On occasion Dask worker memory is not released back to the OS. Setting this value to `0` or some other low number will aggressively and automatically trim the memory. This may yield a more stable, though perhaps slower, performance.
```sh
export MALLOC_TRIM_THRESHOLD_=0
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
Run this script with a `tile_id` argument to create masks from the preproccesed data. Currently the script will create four mask GeoTIFFs for the following conditions: ocean, lake / inland water, L2 Fill (i.e., no data), and a combined mask of the previous conditions. Masks will be written to disk as GeoTIFFs to the `$SCRATCH_DIR/$SNOW_YEAR/masks` directory. Execution time is about 2 minutes per tile.
#### Example Usage
`python compute_masks.py h11v02`

### `compute_snow_metrics.py`
Run this script with a `tile_id` argument to compute snow metrics from the preproccesed data. Outputs will be single-band GeoTIFFs (one per metric per tile) written to the `$SCRATCH_DIR/$SNOW_YEAR/single_metric_geotiffs` directory. The metrics currently computed include (in no particular order):
1. First Snow Day (FSD) of the full snow season (FSS). Also called FSS start day.
2. Last Snow Day (LSD) of the FSS. Also called FSS end day.
3. FSS Range: the length (duration) of the full snow season.
4. Continuous Snow Season (CSS) Start Day: First day of longest CSS segment.
5. CSS End Day: last day of longest CSS segment.
6. CSS Range: the length (duration) of the longest CSS segment.
7. Number of discrete CSS segments.
8. Total CSS Days: summed duration of all CSS segments
9. Number of Snow Days: count of all snow-covered days in a snow year
10. Number of No Snow Days: count of all not snow-covered days in a snow year
11. Number of Cloud Days: count of all cloud-covered days in a snow year

Execution time is about 15 minutes per tile.
#### Example Usage
`python compute_snow_metrics.py h11v02`

### `postprocess.py`
Run this script with no arguments to postprocess all data in the `single_metric_geotiffs` directory. The script spawns subprocesses that call GDAL routines to reproject GeoTIFFs to ESPG:3338, align grids, and mosaic tiles. Outputs are written to compressed GeoTIFFs. Additional tasks will include stacking individual rasters to a final multiband GeoTIFF.
#### Example Usage
`python postprocess.py`

## Other Modules
### `shared_utils.py`
This module contains convenience and utility functions that are used across multiple other scripts and modules. At the moment these mostly are functions for file input and output.

### `gather_uncertainty_data.py`
Gather data for downstream uncertainty analyses. This module will construct GeoTIFFs for maximum cloud persistence and also yield rasters that indicate other anomalous values if they occur anywhere in the time series.
#### Example Usage
`python gather_uncertainty_data.py h11v02`
