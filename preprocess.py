import xarray as xr
import rasterio as rio
import rioxarray
import numpy as np

from datetime import datetime, timedelta
from pathlib import Path

from config import INPUT_DIR, SCRATCH_DIR
from luts import data_variables

def list_input_files(src_dir):
    fps = [x for x in src_dir.glob("*.tif")]
    print(len(fps))
    return fps

def parse_tile(fp):
    return fp.name.split("_")[2]

def parse_date(fp):              
    return fp.name.split("_")[1][1:]

def parse_data_variable(fp):
    return fp.name.split("2D_")[1].split(".")[0][:-9]

def parse_satellite():
    pass

def convert_yyyydoy_to_date(doy_str):
    # Convert YYYY-DOY to a datetime object
    year, doy = int(doy_str[0:4]), int(doy_str[-3:])
    date = datetime(year, 1, 1) + timedelta(days=doy - 1)
    return date.date()