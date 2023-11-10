# VIIRS Snow Metrics for Alaska

## Development Setup
Placeholder: `conda env` setup instructions

Developers and users must set the following environment variables:
### Directory Structure
These variables will be ready by the configuration file. If the directories do not exist they will be created for you at runtime.
##### `INPUT_DIR`
Set to a path where you will download input data from NSIDC. Anticpate needing around 10GB disk space per year for the full AK domain. For a production run there are 82,003 granules of VNP10A1F version 2 totaling ~192 GB, so the download directory (`INPUT_DIR`) needs at least 200 GB free space for a full run. Example:
```sh
export INPUT_DIR=/big_imaginary_disks/VIIRS_L3_snow_cover
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


