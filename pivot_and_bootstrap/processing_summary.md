# Processing Summary

> **Student-facing template (example only):** Replace the bracketed placeholders below with

## Runtime & Memory

- Total runtime: [SECONDS] seconds ([MINUTES] minutes, [HOURS] hours)
- Peak memory: [PEAK_MB] MB ([PEAK_GB] GB)

## Output

- Total rows in pivoted dataframe: [N_PIVOT_ROWS]
- Total rows after aggregation: [N_AGG_ROWS]

## Input vs Discarded

- Total input rows: [N_INPUT_ROWS]
- Rows discarded: [N_DISCARDED] out of [N_INPUT_ROWS] ([PCT_DISCARDED]%)

### Discarded by Reason

- date_inconsistent: [N_DATE_INCONSISTENT]
- datetime_parse_fail: [N_DATETIME_PARSE_FAIL]
- missing_date_taxi_type: [N_MISSING_DATE_TAXI_TYPE]
- other: [N_OTHER] (optional)

## Date Consistency Issues

- Total inconsistent rows: [N_INCONSISTENT_ROWS]
- Files with issues: [N_FILES_WITH_ISSUES]

## Schema Summary

- Shape: [N_ROWS] rows x [N_COLS] columns
- Columns: date, taxi_type, hour_0, hour_1, hour_2, hour_3, hour_4, hour_5, hour_6, hour_7, hour_8, hour_9, hour_10, hour_11, hour_12, hour_13, hour_14, hour_15, hour_16, hour_17, hour_18, hour_19, hour_20, hour_21, hour_22, hour_23
