from pathlib import Path

def parse_date_h5(fp: Path) -> str:
    """Parse the date from an h5 filename.
    Args:
       fp (Path): The file path object.

    Returns:
       str: The date (DOY format) extracted from the filename.
    """
    return fp.name.split(".")[1][1:]

def parse_tile_h5(fp: Path) -> str:
    return fp.name.split(".")[2]


