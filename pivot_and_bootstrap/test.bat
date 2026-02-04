python pivot_all_files.py ^
    --input-dir "s3://dsc291-ucsd/taxi/Dataset/2023/yellow_taxi/" ^
    --output-dir data/remote_test_output_dask_parallel/ ^
    --workers 8