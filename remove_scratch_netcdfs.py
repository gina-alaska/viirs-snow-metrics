import os
from config import snow_year_scratch_dir

netcdf_files = list(snow_year_scratch_dir.rglob("*.nc"))

# prompt to confirm deletion
print(
    f"Are you sure you want to delete {len(netcdf_files)} NetCDF files from {snow_year_scratch_dir}? (yes/no)"
)
confirmation = input()

if confirmation.lower() == "yes":
    for file in netcdf_files:
        os.remove(file)
    print(f"Deleted {len(netcdf_files)} NetCDF files from {snow_year_scratch_dir}.")
else:
    print("Operation cancelled.")
