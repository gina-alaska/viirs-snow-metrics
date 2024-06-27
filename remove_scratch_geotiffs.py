import os
from config import snow_year_scratch_dir

geotiff_files = list(snow_year_scratch_dir.rglob("*.tif"))

# prompt to confirm deletion
print(
    f"Are you sure you want to delete {len(geotiff_files)} GeoTIFF files from {snow_year_scratch_dir}? (yes/no)"
)
confirmation = input()

if confirmation.lower() == "yes":
    for file in geotiff_files:
        os.remove(file)
    print(f"Deleted {len(geotiff_files)} GeoTIFF files from {snow_year_scratch_dir}.")
else:
    print("Operation cancelled.")
