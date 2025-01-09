import unittest
from pathlib import Path
import rasterio as rio
import pickle
import xarray as xr

from shared_utils import open_preprocessed_dataset
from h5_utils import get_attrs_from_h5, create_proj_from_viirs_snow_h5
from config import SNOW_YEAR, preprocessed_dir, mask_dir
from compute_masks import generate_ocean_mask
from convert_nsidc_h5_to_geotiff import convert_data_array_to_geotiff

class UnitTest(unittest.TestCase):
    
    def test_mask(self):
        tile_id = 'h11v02'
        with open("file_dict_h5.pickle", "rb") as handle:
            h5_dict = pickle.load(handle)
        h5_reference = h5_dict[tile_id][0]
        h5_attrs = get_attrs_from_h5(h5_reference)
        crs = create_proj_from_viirs_snow_h5(h5_attrs)
        fp = preprocessed_dir / f"snow_year_{2023}_{tile_id}.nc"
        ds = open_preprocessed_dataset(
            fp, {"x": "auto", "y": "auto"}, "CGF_NDSI_Snow_Cover"
        )
        ocean_mask = generate_ocean_mask(ds)
        ocean_mask.name="ocean_mask"
        ocean_mask = ocean_mask.rio.write_crs(crs)
        convert_data_array_to_geotiff(ocean_mask, Path('./test_ocean_mask.tif'), compress='Deflate', nodata=0)

        

if __name__ == "__main__":
    unittest.main()
