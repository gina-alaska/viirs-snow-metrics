# Snow Metrics User Guide

## GeoTIFF Specifications
### Single band/metric GeoTIFFs
Metrics indicating a discrete day-of-snow-year will range between 213 and 577 because the snow year is defined as August 1 to July 31. August 1 is day-of-standard-year 213 while July 31 is day-of-standard-year 212 (212 + 365 = 577). When $SNOW_YEAR + 1 is a leap year, the maximum value may be 578.

### Composite Snow Metric GeoTIFFs