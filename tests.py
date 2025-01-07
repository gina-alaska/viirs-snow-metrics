import unittest
from pathlib import Path

from shared_utils import list_input_files
from preprocess import construct_file_dict
from h5_utils import parse_date_h5, parse_tile_h5, construct_file_dict_h5

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
        self.assertTrue('h13v02' in file_dict_h5)
        self.assertTrue(len(file_dict_h5['h13v02']) > 1)


    def tearDown(self):
        """
        Clean up after tests if necessary.
        This method runs after each test.
        """
        pass


if __name__ == "__main__":
    unittest.main()
