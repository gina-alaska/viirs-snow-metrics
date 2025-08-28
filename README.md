# VIIRS Snow Metrics for Alaska

## System Requirements
Production runs of this code will execute on GINA's "Elephant" machine. Additional testing may occur on SNAP's "Atlas" compute nodes. These are both Linux machines. Other execution environments are not supported. Users will need ample free disk space (order 500 GB) per snow year. Elephant has 18 cores / 144 CPUs.

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
Set to a path where you will download and reference the input data from NSIDC. Anticipate needing around 25 GB free disk space per snow year to download all tiles for the full Alaska spatial domain as HDF5 files, or 80 GB as GeoTIFFs. Example:
```sh
export INPUT_DIR=/export/datadir/"$USER"_viirs_snow/VIIRS_L3_snow_cover
```
###### `SCRATCH_DIR`
Set the path where you will read/write intermediate data. At least 200 GB will be needed for intermediate files. Example:
```sh
export SCRATCH_DIR=/export/datadir/"$USER"_viirs_snow/scratch
```
The scratch directory is structured as such:
```
/export/datadir/$USER_viirs_snow/scratch
└── $SNOW_YEAR
    ├── masks
    ├── preprocessed
    ├── reprojected_mask_geotiffs
    ├── reprojected_merged_mask_geotiffs
    ├── reprojected_merged_single_metric_geotiffs
    ├── reprojected_merged_uncertainty_geotiffs
    ├── reprojected_single_metric_geotiffs
    ├── reprojected_uncertainty_geotiffs
    ├── single_metric_geotiffs
    └── uncertainty_geotiffs
```
###### `OUTPUT_DIR`
Set to a path where you will write the snow metric outputs to disk. Use a shared disk location so multiple users can examine the output data.
```sh
export OUTPUT_DIR=/export/datadir/"$USER"_viirs_snow/VIIRS_snow_metrics
``` 
Final stacked snow metric GeoTiff files will be created in this directory with the naming structure: 
```{SNOW_YEAR}_VIIRS_snow_metrics_{version}.tif```

#### Runtime Options
###### `SNOW_YEAR`
Set the "snow year" to download and process. Snow year 2015 begins August 1, 2015 and ends on July 31, 2016. Leap days are included.
```sh
export SNOW_YEAR=2015
``` 
###### `MALLOC_TRIM_THRESHOLD_` (optional)
On occasion Dask worker memory is not released back to the OS. Setting this value to `0` or some other low number will aggressively and automatically trim the memory. This may yield a more stable, though perhaps slower, performance.
```sh
export MALLOC_TRIM_THRESHOLD_=0
```

## Usage
#### `download.py`
Run this script with no arguments to download the source dataset from the NSIDC DAAC as HDF5 files. An optional argument for format can download projected GeoTIFF files from the legacy DAAC; however, these methods will not be supported in the futute and do not exist for the VJ110A1F product. The [earthaccess](https://earthaccess.readthedocs.io/) api is used for authentication and downloading HDF5 files. Users will be prompted to enter valid Earthdata credentials; alternatively, a .netrc file or environment variables can be used - see the [authentication guide](https://earthaccess.readthedocs.io/en/stable/howto/authenticate/) for details. Users must have an Earthdata account to download the necessary data. Data will be downloaded to the `$INPUT_DIR/$SNOW_YEAR` directory. The script will ask to wipe the contents of `$INPUT_DIR/$SNOW_YEAR` before proceeding with the download.

If you are asking the service to perform [reformatting (e.g., h5 to GeoTIFF)](https://nsidc.org/data/user-resources/help-center/table-key-value-pair-kvp-operands-subsetting-reformatting-and-reprojection-services) the service will take longer to prepare the order. Consider executing `download.py` in a screen session or similar for GeoTIFF usage - it may take a full day to prepare the order for a download of an entire snow year. The order preparation may take longer than the download itself. A log file (`download.log`) that captures the API endpoints used as well as information about the requested granules will be written to the same directory as the download script.
##### Example Usage
`python download.py`
`python download.py --format tif`
`python download.py --short_name VJ110A1F`

#### `preprocess.py`
Run this script with a `tile_id` argument to preprocess the downloaded data. The script will analyze the data from `$INPUT_DIR` and construct a hash table with keys based on the tile of the source dataset and the data variables (one of `"Algorithm_Bit_Flags_QA", "Basic_QA" "CGF_NDSI_Snow_Cover", "Cloud_Persistence", "Daily_NDSI_Snow_Cover"`) in the HDF5 (Or from the GeoTIFF files for each data variable). The script will construct a time-indexed netCDF file containing the entire set of data for the `SNOW_YEAR` being processed and write the file to `$SCRATCH_DIR/$SNOW_YEAR/preprocessed`. Execution time is about 15 minutes per tile.
##### Example Usage
`python preprocess.py h11v02`
`python preprocess.py h11v02 --format tif`

#### `filter_and_fill.py`
Apply a Savitzky-Golay filter to low illumination observations (solar zenith angles < 70 degrees), fill in observation gaps caused by night and cloud conditions, and write a new netCDF dataset containing the output. There are two arguments: the VIIRS Tile ID and the suffix to tag the output file name with. Execution time can range between 15 and 45 minutes per tile.
##### Example Usage
`python filter_and_fill.py h11v02`

#### `compute_masks.py`
Run this script with a `tile_id` argument to create masks from the preproccesed data. Currently the script will create four mask GeoTIFFs for the following conditions: ocean, lake / inland water, L2 Fill (i.e., no data), and a combined mask of the previous conditions. Masks will be written to disk as GeoTIFFs to the `$SCRATCH_DIR/$SNOW_YEAR/masks` directory. Execution time is about 2 minutes per tile.
##### Example Usage
`python compute_masks.py h11v02`

#### `compute_snow_metrics.py`
Run this script with a `tile_id` argument to compute snow metrics from the preproccesed data. Outputs will be single-band GeoTIFFs (one per metric per tile) written to the `$SCRATCH_DIR/$SNOW_YEAR/single_metric_geotiffs` directory. The metrics currently computed include:
1. First Snow Day (FSD) of the full snow season (FSS). Also called FSS start day.
2. Last Snow Day (LSD) of the FSS. Also called FSS end day.
3. FSS Range: the length (duration) of the full snow season.
4. Continuous Snow Season (CSS) Start Day: First day of longest CSS segment.
5. CSS End Day: last day of longest CSS segment.
6. CSS Range: the length (duration) of the longest CSS segment.
7. Number of Snow Days: count of all snow-covered days in a snow year
8. Number of No Snow Days: count of all not snow-covered days in a snow year
9. Number of discrete CSS segments.
10. Total CSS Days: summed duration of all CSS segments

Execution time is about 15 minutes per tile.
##### Example Usage
`python compute_snow_metrics.py h11v02`

#### `postprocess.py`
Run this script with no arguments to postprocess all data in the `single_metric_geotiffs` directory. The script spawns subprocesses that call GDAL routines to reproject GeoTIFFs to ESPG:3338, align grids, and mosaic tiles. Outputs are written to compressed GeoTIFFs. Individual snow metric rasters are stacked in a final multiband GeoTIFF.
##### Example Usage
`python postprocess.py`

## Other Modules
#### `compute_dark_and_cloud_metrics.py`
This script is used to compute metrics related to dark and cloud observations from a preprocessed single tile dataset. The script is required to develop, tune, and compare algorithms for filtering dark and/or cloudy observations. The input dataset is analyzed and GeoTIFFs with information about snow cover observations around the onset (called "dusk" in the code) and conclusion (called "dawn" in the code) of a darkness or cloud condition are computed. For each of these obscured (darkness and cloud) conditions, the script will yield GeoTIFFs representing the time indicies of the observations prior to ("dusk") and immediately after ("dawn") of the obscured condition as well as the median time index between these values. GeoTIFFs are also created that represent the value of the `CGF_NSDI_SNOWCOVER` variable at the dawn and dusk indices and the binary snow cover status (the variable value with the threshold applied) at these indices. The final GeoTIFF created indicates whether or not the binary snow cover status transitioned (off to on, on to off) during the obscured period. The script accepts user arguments for the input because using the script to tune the filtering algorithm requires computing the metrics for "raw" and "filtered" data. Omitting the input argument will run the script on the unfiltered data by default. Execution time will really depend on the tile that is being processed. Higher latitude tiles with more grid cells that experience long winter darkness periods will take longer. To compute on the smoothed data, just provide the file suffix.
##### Example Usage
`python compute_dark_and_cloud_metrics.py h11v02`

`python compute_dark_and_cloud_metrics.py h11v02 --smoothed_input smoothed_low_illumination`

#### `gather_uncertainty_data.py`
Gather data for downstream uncertainty analyses. This module will read the preprocessed netCDF file and construct GeoTIFFs for maximum cloud persistence and also yield rasters that indicate other anomalous values (e.g., bowtie trim) if they occur anywhere in the time series. Running this script is not strictly required.
##### Example Usage
`python gather_uncertainty_data.py h11v02`

#### `shared_utils.py`
This module contains convenience and utility functions that are used across multiple other scripts and modules, i.e. functions for file input and output.

#### `convert_nsidc_h5_to_geotiff.py`
Convert an HDF5 file into GeoTIFF files for each data variable. Useful for visual examination of daily data for a given tile.
##### Example Usage
`convert_nsidc_h5_to_geotiff.py /path/to/snow_year_input_dir/VNP10A1F.A2013013.h10v02.002.2023165001632.h5 --output-dir /path/to/output/dir/ --epsg 3338 `

#### `remove_scratch_geotiffs.py`
Utility function to remove intermediate GeoTIFF files. Counts number of matching files and prompts for user input before deleting.

#### `remove_scratch_netcdfs.py`
Utility function to remove intermediate netcdf files. Counts number of matching files and prompts for user input before deleting.