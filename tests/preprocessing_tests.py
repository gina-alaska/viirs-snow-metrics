import unittest
from pathlib import Path

from shared_utils import list_input_files, convert_yyyydoy_to_date
from preprocess import construct_file_dict, create_single_tile_dataset
from h5_utils import ( 
    parse_date_h5, parse_tile_h5, construct_file_dict_h5,
    extract_coords_from_viirs_snow_h5, create_proj_from_viirs_snow_h5,
    create_xarray_from_viirs_snow_h5, get_attrs_from_h5,
    initialize_transform_h5, make_sorted_h5_stack
)
from preprocess_h5 import create_single_tile_dataset_from_h5

class UnitTest(unittest.TestCase):

    def setUp(self):
        """
        Set up any test dependencies, configurations, or mock data.
        This method runs before each test.
        """
        pass


    def test_list_input_files(self):
        src_tif = Path('/export/datadir/ojlarson_viirs_snow/VIIRS_L3_snow_cover/2016')
        src_h5 = Path('/export/datadir/ojlarson_viirs_snow/VIIRS_L3_snow_cover/2022')
        self.assertTrue(list_input_files(src_tif))
        self.assertTrue(list_input_files(src_h5, extension='*.h5'))

    def test_construct_file_dict(self):
        geotiffs = list_input_files(Path('/export/datadir/ojlarson_viirs_snow/VIIRS_L3_snow_cover/2016'))
        geotiff_di = construct_file_dict(geotiffs)
        self.assertTrue(geotiff_di['h11v02']['CGF_NDSI_Snow_Cover'])

    def test_process_date_h5(self):
        fp_h5 = Path('/export/datadir/ojlarson_viirs_snow/VIIRS_L3_snow_cover/2022/VNP10A1F.A2023016.h13v02.002.2023096053547.h5')
        self.assertEqual(parse_date_h5(fp_h5), '2023016')

    def test_parse_tile_h5(self):
        fp_h5 = Path('/export/datadir/ojlarson_viirs_snow/VIIRS_L3_snow_cover/2022/VNP10A1F.A2023016.h13v02.002.2023096053547.h5')
        self.assertEqual(parse_tile_h5(fp_h5), 'h13v02')

    def test_construct_file_dict_h5(self):
        src_h5 = Path('/export/datadir/ojlarson_viirs_snow/VIIRS_L3_snow_cover/2022')
        fps_h5 = list_input_files(src_h5, extension='*.h5')
        file_dict_h5 = construct_file_dict_h5(fps_h5)
        self.assertIn('h13v02', file_dict_h5)
        self.assertTrue(len(file_dict_h5['h13v02']) > 1)
    
    def test_extract_coords_from_viirs_snow_h5(self):
        fp_h5 = Path('/export/datadir/ojlarson_viirs_snow/VIIRS_L3_snow_cover/2022/VNP10A1F.A2023016.h13v02.002.2023096053547.h5')
        x_dim, y_dim = extract_coords_from_viirs_snow_h5(fp_h5)
        self.assertEqual(len(x_dim), 3000)
        self.assertEqual(len(y_dim), 3000)

    def test_get_attrs_from_h5(self):
        fp_h5 = Path('/export/datadir/ojlarson_viirs_snow/VIIRS_L3_snow_cover/2022/VNP10A1F.A2023016.h13v02.002.2023096053547.h5')
        self.assertIsInstance(get_attrs_from_h5(fp_h5), dict)

    def test_create_proj_from_viirs_snow_h5(self):
        fp_h5 = Path('/export/datadir/ojlarson_viirs_snow/VIIRS_L3_snow_cover/2022/VNP10A1F.A2023016.h13v02.002.2023096053547.h5')
        crs = create_proj_from_viirs_snow_h5(get_attrs_from_h5(fp_h5))
        self.assertIn('sinu', crs.to_string())
    
    def test_create_xarray_from_viirs_snow_h5(self):
        fp_h5 = Path('/export/datadir/ojlarson_viirs_snow/VIIRS_L3_snow_cover/2022/VNP10A1F.A2023016.h13v02.002.2023096053547.h5')
        dataset = create_xarray_from_viirs_snow_h5(fp_h5)
        self.assertIn('CGF_NDSI_Snow_Cover', dataset.data_vars)

    def test_initialize_transform_h5(self):
        fp_h5 = Path('/export/datadir/ojlarson_viirs_snow/VIIRS_L3_snow_cover/2022/VNP10A1F.A2023016.h13v02.002.2023096053547.h5')
        x_dim, y_dim = extract_coords_from_viirs_snow_h5(fp_h5)
        self.assertTrue(initialize_transform_h5(x_dim, y_dim))
        
    def test_create_single_tile_dataset(self):
        geotiffs = list_input_files(Path('/export/datadir/ojlarson_viirs_snow/VIIRS_L3_snow_cover/2016'))[:500]
        geotiff_di = construct_file_dict(geotiffs)
        tile_ds = create_single_tile_dataset(geotiff_di, 'h11v02')
        print(tile_ds)
        self.assertTrue(tile_ds)

    def test_create_single_tile_dataset_from_h5(self):
        src_h5 = Path('/export/datadir/ojlarson_viirs_snow/VIIRS_L3_snow_cover/2022')
        fps_h5 = list_input_files(src_h5, extension='*.h5')[:100]
        file_dict_h5 = construct_file_dict_h5(fps_h5)
        tile_ds = create_single_tile_dataset_from_h5(file_dict_h5, 'h11v02')
        print(tile_ds)
        self.assertTrue(tile_ds)

    def test_make_sorted_h5_stack(self):
        src_h5 = Path('/export/datadir/ojlarson_viirs_snow/VIIRS_L3_snow_cover/2022')
        fps_h5 = list_input_files(src_h5, extension='*.h5')[:200]
        file_dict_h5 = construct_file_dict_h5(fps_h5)
        dates = [
            convert_yyyydoy_to_date(parse_date_h5(x))
            for x in file_dict_h5['h11v02']
        ]
        dates.sort()
        yyyydoy_strings = [d.strftime("%Y") + d.strftime("%j") for d in dates]
        variable_path=r"/HDFEOS/GRIDS/VIIRS_Grid_IMG_2D/Data Fields/CGF_NDSI_Snow_Cover"
        self.assertTrue(make_sorted_h5_stack(fps_h5, yyyydoy_strings, variable_path))

    def tearDown(self):
        """
        Clean up after tests if necessary.
        This method runs after each test.
        """
        pass


if __name__ == "__main__":
    unittest.main()
