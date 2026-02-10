# Processing Summary

Performance metrics for a single run of the entire pipeline using:
```bash
python3 pivot_all_files.py \
    --input-dir "s3://dsc291-ucsd/taxi/Dataset/" \
    --output-dir "s3://dsc291-taxi/taxi-output2/" \
    --workers 16 2>&1 | tee output-mem.txt
```
on an AWS `r8i.4xlarge` instance (16 vCPUS, 128 GB vRAM)

## Runtime & Memory

- Total Runtime: 226.08 seconds (3.77 minutes, 0 hours)
- Peak Memory: 126,307.64 MB (123.35 GB)

## Output

- Total Rows for Pivoted DataFrame: 3,226,594,820
- Total Rows after Aggregation: 1,513,805

## Input vs Discarded

- Total Input Rows: 3,410,052,578
- Rows Discarded: 183,457,758 out of 3,410,052,578 (5.37%)

### Discarded by Reason

- Month Mismatch: 17,064
- Invalid Pickup Location: 181,905,094
- Low Ride Count: 1,535,600

## Date Consistency Issues
- Total Inconsistent Rows: 17,064

## Input/Output Summary

| Metric | Count |
|--------|-------|
| Total Input Rows | 3,410,052,578 |
| Intermediate Pivoted Rows | 1,513,805 |
| Total Output Rows | 1,420,233 |

## Discarded Rows Breakdown

| Reason | Count | Percentage |
|--------|-------|------------|
| Month Mismatch | 17,064 | 0.0005% |
| Invalid Pickup Location | 181,905,094 | 5.33%
| Low Count (< 50 rides) | 1,535,600 | 0.04% |
| Parse Failures | 0 | 0% |
| **Total Discarded** | 183,457,758 | 5.37% |

## Schema Summary

- Shape: 1420233 rows x 27 columns
- Columns: date, taxi_type, hour_0, hour_1, hour_2, hour_3, hour_4, hour_5, hour_6, hour_7, hour_8, hour_9, hour_10, hour_11, hour_12, hour_13, hour_14, hour_15, hour_16, hour_17, hour_18, hour_19, hour_20, hour_21, hour_22, hour_23

### # of Rows per Year

| Year | # of Rows |
|------|-------------|
| 2009 | 38,418 |
| 2010 | 38,720 |
| 2011 | 40,758 |
| 2012 | 39,223 |
| 2013 | 37,713 |
| 2014 | 69,821 |
| 2015 | 136,928 |
| 2016 | 150,127 |
| 2017 | 147,419 |
| 2018 | 152,315 |
| 2019 | 115,616 |
| 2020 | 120,564 |
| 2021 | 120,581 |
| 2022 | 121,480 |
| 2023 | 90,550 |

### # of Rows per Taxi Type

| Taxi Type | # of Rows |
|-----------|-------------|
| Yellow | 470,117 |
| Green | 185,467 |
| FHV | 764,649 |

### # of Rows per Taxi Type by Year

| Year | FHV | Green | Yellow |
|------|-----|-------|--------|
| 2009 | 0 | 0 | 38,418 |
| 2010 | 0 | 0 | 38,720 |
| 2011 | 0 | 0 | 40,758 |
| 2012 | 0 | 0 | 39,223 |
| 2013 | 0 | 0 | 37,713 |
| 2014 | 0 | 31,505 | 38,316 |
| 2015 | 66,043 | 33,695 | 37,190 |
| 2016 | 84,410 | 29,323 | 36,394 |
| 2017 | 89,389 | 23,441 | 34,589 |
| 2018 | 91,759 | 26,576 | 33,980 |
| 2019 | 92,168 | 23,448 | 0 |
| 2020 | 90,499 | 6,434 | 23,631 |
| 2021 | 90,827 | 3,901 | 25,853 |
| 2022 | 91,092 | 4,089 | 26,299 |
| 2023 | 68,462 | 3,055 | 19,033 |