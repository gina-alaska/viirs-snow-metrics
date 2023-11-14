"""Look-Up Tables for VIIRS Snow Metric Computations"""

short_name = "VNP10A1F"

parameter_sets = {
    "dev_params": {
        "bbox": "-146,65,-145,66",
        "start_date": "2013-01-01T00:00:00Z",
        "end_date": "2014-12-31T23:00:00Z",
    },
    "prod_params": {
        "bbox": "172,51,-130,72",
        "start_date": "2012-01-01",
        "end_date": "2022-12-31",
    },
}

# CP note: some of these could be remapped to a single int for NoData
cgf_snow_cover_codes = {
    **{i: "NDSI snow cover valid" for i in range(101)},
    201: "No decision",
    211: "Night",
    237: "Lake / Inland water",
    239: "Ocean",
    250: "Cloud",
    251: "Missing L1B data",
    252: "L1B data failed calibration",
    253: "Onboard VIIRS bowtie trim",
    254: "L1B fill",
    255: "L2 fill",
}

snow_cover_threshold = 50
