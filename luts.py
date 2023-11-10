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
