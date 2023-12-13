"""Functions that will be used across multiple modules."""


def open_preprocessed_dataset(tile, chunk_dict):
    """Open a preprocessed dataset for a given tile.

    Args:
        tile (str): The tile identifier.
        chunk_dict (dict): how to chunk the dataset, like `{"time": 52}`

    Returns:
       xarray.Dataset: The chunked dataset.
    """
    fp = f"snow_year_{SNOW_YEAR}_{tile}.nc"
    logging.info(f"Opening preprocessed file {fp} as chunked Dataset...")

    with xr.open_dataset(preprocessed_dir / fp).CGF_NDSI_Snow_Cover.chunk(
        chunk_dict
    ) as ds_chunked:
        return ds_chunked


# write geotiff with a tag()


# spinning up the raster creation profile()
