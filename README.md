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

Developers and users must set the following environment variables:
### Directory Structure
These variables will be read by the configuration file. If the directories do not exist they will be created for you at runtime.
##### `INPUT_DIR`
Set to a path where you will download input data from NSIDC. Anticpate needing around 10GB disk space per year for the full AK domain. For a production run there are 82,003 granules of VNP10A1F version 2 totaling ~192 GB, so the download directory (`INPUT_DIR`) needs at least 200 GB free space for a full run. Example:
```sh
export INPUT_DIR=/export/datadir/your_viirs_dir/VIIRS_L3_snow_cover
```
##### `SCRATCH_DIR`
Set to the path where you will read/write preprocessed data prior to computation of the actual metrics. Something like:
```sh
export SCRATCH_DIR=$HOME/VIIRS_snow_metrics/scratch
```
##### `OUTPUT_DIR`
Set to a path where you will write the snow metric outputs to disk. Consider using a shared disk location so multiple users can examine the output data.
```sh
export OUTPUT_DIR=/some_shared_disk/VIIRS_snow_metrics
``` 
### Runtime Options
##### `DEV_MODE` (optional)
Set to True (note this will be passed as a `string` type rather than `bool`) to work on a smaller chunk of data both spatially and temporally for the purpose of improving development speed. Default is True. Setting to False will trigger a production run.
```sh
export DEV_MODE=True
```

## Usage
### `download.py`
Run this script with no arguments to download the source dataset from the NSIDC DAAC. Users will be prompted to enter valid Earthdata credentials. Users must have an Earthdata account to download the necessary data. Data will be downloaded to the `$INPUT_DIR` directory. The script will ask to wipe the contents of `$INPUT_DIR` before proceeding with the download. Total download time will of course depend on your connection speed, but it will also depend on how busy the upstream data service is and how complicated your data orders[] are. For example, if you are asking the service to perform [reformatting (e.g., h5 to GeoTIFF)](https://nsidc.org/data/user-resources/help-center/table-key-value-pair-kvp-operands-subsetting-reformatting-and-reprojection-services) the service will take longer to prepare the order. Consider executing `download.py` in a screen session or similar. It may take a full day to prepare the order for a download of the full time series, and the order preparation may take longer than the download itself. A log file (`download.log`) will be written to the same directory as the download script that captures the API endpoints used as well as information about the requested granules.
#### Example Usage
`python download.py`



