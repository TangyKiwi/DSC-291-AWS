python pivot_all_files.py ^
    --input-dir "s3://dsc291-ucsd/taxi/Dataset/2009/yellow_taxi/" ^
    --output-dir data/v2_test_schema/ ^
    --workers 8 ^
    --keep-intermediate