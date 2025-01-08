import unittest
from pathlib import Path
import shutil
import time

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
        self.assertEqual(len(snow_year_chunks), 12)

    def test_cms_search(self):
        snow_year_chunks = generate_monthly_dl_chunks(2023)
        short_name = "vnp10a1f"
        time_start = snow_year_chunks[3][0]
        time_end = snow_year_chunks[3][1]
        bounding_box = "-150,60,-145,65"

        url_list = cmr_search(
            short_name, time_start, time_end, bounding_box=bounding_box
        )
        self.assertEqual(len(url_list), 360)
        self.assertIn("VNP10A1F", url_list[0])
        short_name = "VJ110a1f"
        url_list = cmr_search(
            short_name, time_start, time_end, bounding_box=bounding_box
        )
        self.assertEqual(len(url_list), 360)
        self.assertIn("VJ110A1F", url_list[0])

    def test_cmr_download(self):
        snow_year_chunks = generate_monthly_dl_chunks(2023)
        short_name = "vnp10a1f"
        time_start = snow_year_chunks[3][0]
        time_end = snow_year_chunks[3][1]
        bounding_box = "-140,66,-139,67"

        url_list = cmr_search(
            short_name, time_start, time_end, bounding_box=bounding_box
        )
        self.assertTrue(
            cmr_download(url_list[:2], quiet=False, download_dir="./test_dl")
        )
        shutil.rmtree("./test_dl")

    def test_tiles_in_bbox(self):
        snow_year_chunks = generate_monthly_dl_chunks(2023)
        short_name = "vnp10a1f"
        time_start = snow_year_chunks[3][0]
        time_end = snow_year_chunks[3][1]
        bounding_box = parameter_sets["prod_params"]["bbox"]

        url_list = cmr_search(
            short_name, time_start, time_end, bounding_box=bounding_box
        )
        start_time = time.time()
        good_tiles = [
            url
            for url in url_list
            if parse_tile_h5(Path(url)) in needed_tile_ids
            and Path(url).suffix != ".xml"
        ]
        print(f"List comprehension: {time.time() - start_time:.4f} seconds")

        start_time = time.time()
        good_tiles = []
        for x in url_list:
            path_x = Path(x)
            if parse_tile_h5(path_x) in needed_tile_ids and path_x.suffix != ".xml":
                good_tiles.append(x)
        print(f"Manual append: {time.time() - start_time:.4f} seconds")

        unique_tiles = set([parse_tile_h5(Path(x)) for x in good_tiles])
        print(unique_tiles)
        self.assertEqual(unique_tiles, needed_tile_ids)

    def tearDown(self):
        """
        Clean up after tests if necessary.
        This method runs after each test.
        """
        pass


if __name__ == "__main__":
    unittest.main()
