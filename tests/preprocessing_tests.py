import unittest
from pathlib import Path
import xarray as xr
import dask.array as da
import time

from config import preprocessed_dir, SNOW_YEAR
from shared_utils import (
    list_input_files,
    convert_yyyydoy_to_date,
    write_single_tile_xrdataset,
    open_preprocessed_dataset,
)
from preprocess import (
    construct_file_dict,
    create_single_tile_dataset,
    make_sorted_raster_stack,
    parse_date,
)
from h5_utils import (
    parse_date_h5,
    parse_tile_h5,
    construct_file_dict_h5,
    extract_coords_from_viirs_snow_h5,
    create_proj_from_viirs_snow_h5,
    create_xarray_from_viirs_snow_h5,
    get_attrs_from_h5,
    initialize_transform_h5,
    make_sorted_h5_stack,
)
from preprocess_h5 import create_single_tile_dataset_from_h5


class UnitTest(unittest.TestCase):

    def setUp(self):
        """
        Set up any test dependencies, configurations, or mock data.
        This method runs before each test.
        """
        pass

    def set_src_tif(self):
        src_tif = Path("/export/datadir/ojlarson_viirs_snow/VIIRS_L3_snow_cover/2016")
        return src_tif

    def set_src_h5(self):
        src_h5 = Path("/export/datadir/ojlarson_viirs_snow/VIIRS_L3_snow_cover/2022")
        return src_h5

    def set_fp_h5(self):
        fp_h5 = Path(
            "/export/datadir/ojlarson_viirs_snow/VIIRS_L3_snow_cover/2022/VNP10A1F.A2023016.h13v02.002.2023096053547.h5"
        )
        return fp_h5

    def test_list_input_files(self):
        src_tif = self.set_src_tif()
        src_h5 = self.set_src_h5()
        self.assertTrue(list_input_files(src_tif))
        self.assertTrue(list_input_files(src_h5, extension="*.h5"))

    def test_construct_file_dict(self):
        geotiffs = list_input_files(self.set_src_tif())
        geotiff_di = construct_file_dict(geotiffs)
        self.assertTrue(geotiff_di["h11v02"]["CGF_NDSI_Snow_Cover"])

    def test_process_date_h5(self):
        fp_h5 = self.set_fp_h5()
        self.assertEqual(parse_date_h5(fp_h5), "2023016")

    def test_parse_tile_h5(self):
        fp_h5 = self.set_fp_h5()
        self.assertEqual(parse_tile_h5(fp_h5), "h13v02")

    def test_construct_file_dict_h5(self):
        src_h5 = self.set_src_h5()
        fps_h5 = list_input_files(src_h5, extension="*.h5")
        file_dict_h5 = construct_file_dict_h5(fps_h5)
        self.assertIn("h13v02", file_dict_h5)
        self.assertTrue(len(file_dict_h5["h13v02"]) > 1)

    def test_extract_coords_from_viirs_snow_h5(self):
        fp_h5 = self.set_fp_h5()
        x_dim, y_dim = extract_coords_from_viirs_snow_h5(fp_h5)
        # print('X', x_dim.values)
        # print('Y', y_dim.values)
        self.assertEqual(len(x_dim), 3000)
        self.assertEqual(len(y_dim), 3000)

    def test_get_attrs_from_h5(self):
        fp_h5 = self.set_fp_h5()
        self.assertIsInstance(get_attrs_from_h5(fp_h5), dict)

    def test_create_proj_from_viirs_snow_h5(self):
        fp_h5 = self.set_fp_h5()
        crs = create_proj_from_viirs_snow_h5(get_attrs_from_h5(fp_h5))
        self.assertIn("sinu", crs.to_string())

    def test_create_xarray_from_viirs_snow_h5(self):
        fp_h5 = self.set_fp_h5()
        dataset = create_xarray_from_viirs_snow_h5(fp_h5)
        self.assertIn("CGF_NDSI_Snow_Cover", dataset.data_vars)

    def test_initialize_transform_h5(self):
        fp_h5 = self.set_fp_h5()
        x_dim, y_dim = extract_coords_from_viirs_snow_h5(fp_h5)
        transform = initialize_transform_h5(x_dim, y_dim)
        # print("Transform", transform, type(transform))
        self.assertTrue(initialize_transform_h5(x_dim, y_dim))

    def test_create_single_tile_dataset(self):
        geotiffs = list_input_files(self.set_src_tif())[:500]
        geotiff_di = construct_file_dict(geotiffs)
        tile_ds = create_single_tile_dataset(geotiff_di, "h11v02")
        # print(tile_ds)
        self.assertTrue(tile_ds)
        # print("DIMS_TIFF:", tile_ds.dims)
        # print("DATA_VARS_TIFF:", tile_ds.data_vars)
        # for var in tile_ds.data_vars:
        #    print("VAR_CHUNKS_TIFF", var, tile_ds[var].chunks)
        write_single_tile_xrdataset(tile_ds, "h11v02", "testingTIFF")
        fp = preprocessed_dir / f"snow_year_{SNOW_YEAR}_h11v02_testingTIFF.nc"
        new_ds = open_preprocessed_dataset(
            fp, {"x": "auto", "y": "auto"}, "CGF_NDSI_Snow_Cover"
        )
        print("FP_TIFF")
        print(new_ds)
        print(new_ds.rio.transform())

    def test_create_single_tile_dataset_from_h5(self):
        src_h5 = self.set_src_h5()
        fps_h5 = list_input_files(src_h5, extension="*.h5")[:100]
        file_dict_h5 = construct_file_dict_h5(fps_h5)
        tile_ds = create_single_tile_dataset_from_h5(file_dict_h5, "h11v02")
        # print(tile_ds)
        self.assertIsInstance(tile_ds, xr.Dataset)
        self.assertTrue(tile_ds)
        print(tile_ds)
        tile_ds.to_netcdf("./test_h5.nc")
        new_ds = open_preprocessed_dataset(
            "./test_h5.nc", {"x": "auto", "y": "auto"}, "CGF_NDSI_Snow_Cover"
        )
        print("New Dataset")
        print(new_ds)
        print(new_ds.rio.transform())
        print(new_ds.rio.crs)
        # print("Transform", tile_ds.rio.transform())
        # fp_h5 = self.set_fp_h5()
        # x_dim, y_dim = extract_coords_from_viirs_snow_h5(fp_h5)
        # transform = initialize_transform_h5(x_dim, y_dim)
        # print("Transform", transform, type(transform))
        # tile_ds.rio.write_transform(transform, inplace=True)
        # print("Transform", tile_ds.rio.transform())
        # print("DIMS:", tile_ds.dims)
        # print("DATA_VARS:", tile_ds.data_vars)
        # for var in tile_ds.data_vars:
        #    print("VAR_CHUNKS", var, tile_ds[var].chunks)
        write_single_tile_xrdataset(tile_ds, "h11v02", "testingH5")
        fp = preprocessed_dir / f"snow_year_{SNOW_YEAR}_h11v02_testingH5.nc"
        new_ds = open_preprocessed_dataset(
            fp, {"x": "auto", "y": "auto"}, "CGF_NDSI_Snow_Cover"
        )
        print("FP")
        print(new_ds)
        print(new_ds.rio.transform())

    def test_make_sorted_h5_stack(self):
        src_h5 = self.set_src_h5()
        fps_h5 = list_input_files(src_h5, extension="*.h5")[:200]
        file_dict_h5 = construct_file_dict_h5(fps_h5)
        dates = [
            convert_yyyydoy_to_date(parse_date_h5(x)) for x in file_dict_h5["h11v02"]
        ]
        dates.sort()
        yyyydoy_strings = [d.strftime("%Y") + d.strftime("%j") for d in dates]
        variable_path = (
            r"/HDFEOS/GRIDS/VIIRS_Grid_IMG_2D/Data Fields/CGF_NDSI_Snow_Cover"
        )
        h5_stack = make_sorted_h5_stack(fps_h5, yyyydoy_strings, variable_path)
        # print("H5_STACK", type(h5_stack), len(h5_stack), type(h5_stack[0]), h5_stack[0].shape)#, h5_stack[0].chunks)
        # h5_stack_da = da.stack(h5_stack, axis=0).rechunk({0: len(h5_stack)})
        # print(f"Shape: {h5_stack_da.shape}, Chunks: {h5_stack_da.chunks}")

        self.assertTrue(h5_stack)

    def test_make_sorted_raster_stack(self):
        src_tif = self.set_src_tif()
        fps_tif = list_input_files(src_tif, extension="*.tif")[:200]
        file_dict_h5 = construct_file_dict(fps_tif)
        dates = [
            convert_yyyydoy_to_date(parse_date(x))
            for x in file_dict_h5["h11v02"]["CGF_NDSI_Snow_Cover"]
        ]
        dates.sort()
        yyyydoy_strings = [d.strftime("%Y") + d.strftime("%j") for d in dates]
        raster_stack = make_sorted_raster_stack(fps_tif, yyyydoy_strings)
        # print("RASTER_STACK", type(raster_stack), len(raster_stack), type(raster_stack[0]), raster_stack[0].shape)
        self.assertTrue(raster_stack)

    def test_dask_h5_stack(self):
        src_h5 = self.set_src_h5()
        fps_h5 = list_input_files(src_h5, extension="*.h5")[:200]
        file_dict_h5 = construct_file_dict_h5(fps_h5)
        dates = [
            convert_yyyydoy_to_date(parse_date_h5(x)) for x in file_dict_h5["h11v02"]
        ]
        dates.sort()
        yyyydoy_strings = [d.strftime("%Y") + d.strftime("%j") for d in dates]
        variable_path = (
            r"/HDFEOS/GRIDS/VIIRS_Grid_IMG_2D/Data Fields/CGF_NDSI_Snow_Cover"
        )
        start_time = time.time()
        h5_stack = make_sorted_h5_stack(fps_h5, yyyydoy_strings, variable_path)
        print(f"H5 stack took: {time.time() - start_time:.4f}")
        start_time = time.time()
        h5_stack_da = make_sorted_h5_stack(
            fps_h5, yyyydoy_strings, variable_path, lazy=True
        )
        print(f"H5 DA stack took: {time.time() - start_time:.4f}")
        ## Build this test out if working on dask array version of stack

        # print("H5_STACK", type(h5_stack), len(h5_stack), type(h5_stack[0]), h5_stack[0].shape)#, h5_stack[0].chunks)
        # print("H5_STACK_DA", type(h5_stack_da), len(h5_stack_da), type(h5_stack_da[0]), h5_stack_da[0].shape, h5_stack_da[0].chunks)
        # print({"h5_test": (["time", "y", "x"], da.array(h5_stack))})
        # print({"h5_da_test": (["time", "y", "x"], da.stack(h5_stack_da, axis=0).rechunk({0: len(h5_stack)}))})

        self.assertTrue(h5_stack)

    def tearDown(self):
        """
        Clean up after tests if necessary.
        This method runs after each test.
        """
        pass


if __name__ == "__main__":
    unittest.main()
