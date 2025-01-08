import unittest
import random
import os
from pathlib import Path

from config import (
    snow_year_input_dir,
    preprocessed_dir,
    SNOW_YEAR,
)
from luts import needed_tile_ids
from shared_utils import open_preprocessed_dataset, convert_yyyydoy_to_date
from h5_utils import parse_date_h5, parse_tile_h5, get_data_array_from_h5


class UnitTest(unittest.TestCase):
    def test_preprocessed_cube(self):
        h5_files = list(snow_year_input_dir.glob("*.h5"))
        if h5_files:
            random_h5 = random.choice(h5_files)
            print(f"Random .h5 file: {random_h5}")
        else:
            print("No .h5 files found in the directory.")

        h5_tile_id = parse_tile_h5(random_h5)
        h5_date = convert_yyyydoy_to_date(parse_date_h5(random_h5))

        print(h5_date, h5_tile_id)

        h5_data = get_data_array_from_h5(
            random_h5,
            r"/HDFEOS/GRIDS/VIIRS_Grid_IMG_2D/Data Fields/CGF_NDSI_Snow_Cover",
        )
        print(h5_data)

        fp = preprocessed_dir / f"snow_year_{SNOW_YEAR}_{h5_tile_id}.nc"
        snow_ds = open_preprocessed_dataset(
            fp, {"x": "auto", "y": "auto"}, "CGF_NDSI_Snow_Cover"
        )
        print(snow_ds)
        ds_arr = snow_ds.sel(time=h5_date).all()
        print(ds_arr)
        self.assertEqual(h5_data, ds_arr)


if __name__ == "__main__":
    unittest.main()
