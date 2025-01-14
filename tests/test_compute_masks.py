import unittest
from pathlib import Path
import pickle
import xarray as xr
import rioxarray as rio
import numpy as np
from affine import Affine

from shared_utils import open_preprocessed_dataset
from h5_utils import (
    get_attrs_from_h5,
    create_proj_from_viirs_snow_h5,
    convert_data_array_to_geotiff,
    initialize_transform_h5,
    write_tagged_geotiff_from_data_array,
)
from config import SNOW_YEAR, preprocessed_dir, mask_dir
from compute_masks import generate_ocean_mask, fetch_raster_profile
from compute_masks_h5 import process_masks


class UnitTest(unittest.TestCase):

    def setUp(self):
        self.tile_id = "h11v02"

    def test_mask(self):
        tile_id = "h11v02"
        fp = preprocessed_dir / f"snow_year_{2023}_{tile_id}.nc"
        ds = open_preprocessed_dataset(
            fp, {"x": "auto", "y": "auto"}, "CGF_NDSI_Snow_Cover"
        )
        self.assertAlmostEqual(370.65, round(ds.rio.transform()[0], 2))
        self.assertIn("Sinusoidal", ds.rio.crs.to_string())
        ocean_mask = generate_ocean_mask(ds)

        self.assertEqual(ocean_mask.rio.transform(), ds.rio.transform())
        # convert_data_array_to_geotiff(ocean_mask, Path('./test_ocean_mask.tif'), nodata=0)

    def test_combined_mask(self):
        tile_id = "h11v02"
        fp = preprocessed_dir / f"snow_year_{2023}_{tile_id}.nc"
        ds = open_preprocessed_dataset(
            fp, {"x": "auto", "y": "auto"}, "CGF_NDSI_Snow_Cover"
        )
        ocean_mask, inland_water_mask, l2_mask, combined_mask = process_masks(ds)
        self.assertEqual("ocean_mask", ocean_mask.name)
        self.assertEqual(0, ocean_mask.rio.nodata)
        self.assertIsInstance(combined_mask, xr.DataArray)
        write_tagged_geotiff_from_data_array(
            Path("."),
            tile_id,
            "mask",
            "combined",
            2023,
            combined_mask,
            compress="Deflate",
            dtype="int16",
        )


if __name__ == "__main__":
    unittest.main()
