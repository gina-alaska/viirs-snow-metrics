import unittest
from pathlib import Path
import shutil

from download import generate_monthly_dl_chunks
from NSIDC_download import cmr_search, cmr_download
from luts import parameter_sets, needed_tile_ids
from h5_utils import parse_tile_h5

class UnitTest(unittest.TestCase):

    def setUp(self):
        """
        Set up any test dependencies, configurations, or mock data.
        This method runs before each test.
        """
        pass

    def test_generate_monthly_dl_chunks(self):
        snow_year_chunks = generate_monthly_dl_chunks(2023)
        print("SNC", snow_year_chunks)
        self.assertEqual(len(snow_year_chunks), 12)

    def test_cms_search(self):
        snow_year_chunks = generate_monthly_dl_chunks(2023)
        short_name = 'vnp10a1f'
        time_start = snow_year_chunks[3][0]
        time_end = snow_year_chunks[3][1]
        bounding_box = '-150,60,-145,65'

        url_list = cmr_search(
                short_name,
                time_start,
                time_end,
                bounding_box=bounding_box
            )
        self.assertEqual(len(url_list), 360)
        self.assertIn('VNP10A1F.A2024092.h08v03.002.2024093202023.h5', url_list[0])
        short_name = 'VJ110a1f'
        url_list = cmr_search(
                short_name,
                time_start,
                time_end,
                bounding_box=bounding_box
            )
        self.assertEqual(len(url_list), 360)
        self.assertIn('VJ110A1F.A2024092.h08v03.002.2024103112026.h5', url_list[0])

    def test_cmr_download(self):
        snow_year_chunks = generate_monthly_dl_chunks(2023)
        short_name = 'vnp10a1f'
        time_start = snow_year_chunks[3][0]
        time_end = snow_year_chunks[3][1]
        bounding_box = '-140,66,-139,67'

        url_list = cmr_search(
                short_name,
                time_start,
                time_end,
                bounding_box=bounding_box
            )
        cmr_download(url_list[:2], quiet=False, download_dir='./test_dl')
        shutil.rmtree('./test_dl')

    def test_tiles_in_bbox(self):
        snow_year_chunks = generate_monthly_dl_chunks(2023)
        short_name = 'vnp10a1f'
        time_start = snow_year_chunks[3][0]
        time_end = snow_year_chunks[3][1]
        bounding_box = parameter_sets['prod_params']['bbox']

        url_list = cmr_search(
                short_name,
                time_start,
                time_end,
                bounding_box=bounding_box
            )
        unique_tiles = set([parse_tile_h5(Path(x)) for x in url_list])
        self.assertEqual(unique_tiles, needed_tile_ids)
        


    def tearDown(self):
        """
        Clean up after tests if necessary.
        This method runs after each test.
        """
        pass


if __name__ == "__main__":
    unittest.main()
