import unittest
import random
import os
from pathlib import Path
import pandas as pd
import numpy as np

from config import (
    snow_year_input_dir,
    preprocessed_dir,
    SNOW_YEAR,
)
from luts import needed_tile_ids, data_variables
from shared_utils import open_preprocessed_dataset, convert_yyyydoy_to_date
from h5_utils import parse_date_h5, parse_tile_h5, get_data_array_from_h5


class UnitTest(unittest.TestCase):

    def get_random_h5_array(self, variable, tile_id="h11v02"):

        h5_files = list(snow_year_input_dir.glob(f"*{tile_id}*.h5"))
        if h5_files:
            random_h5 = random.choice(h5_files)
            print(f"Random .h5 file: {random_h5}")
        else:
            print("No .h5 files found in the directory.")
        h5_tile_id = parse_tile_h5(random_h5)
        h5_date = pd.to_datetime(convert_yyyydoy_to_date(parse_date_h5(random_h5)))

        h5_var_path = Path(r"/HDFEOS/GRIDS/VIIRS_Grid_IMG_2D/Data Fields") / variable

        h5_data = get_data_array_from_h5(
            random_h5,
            str(h5_var_path),
        )
        return h5_tile_id, h5_date, h5_data

    def test_preprocessed_cube(self):

        for i in range(5):

            variable = random.choice(data_variables)

            h5_tile_id, h5_date, h5_data = self.get_random_h5_array(variable)

            fp = preprocessed_dir / f"snow_year_{SNOW_YEAR}_{h5_tile_id}.nc"
            print(f"Comparing {variable} on {h5_date} to {fp}")
            snow_ds = open_preprocessed_dataset(
                fp, {"x": "auto", "y": "auto"}, variable
            )

            ds_arr = snow_ds.sel(time=h5_date, method="nearest")

            self.assertTrue(np.all(h5_data == ds_arr.values))


if __name__ == "__main__":
    unittest.main()
