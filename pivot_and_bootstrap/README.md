# NYC Taxi Pipeline

Pipeline that processes NYC TLC taxi trip Parquet data: 
1) **Pivots** trip-level records into (date × taxi_type × pickup_place × hour) counts 
2) **Generates a single wide table** corresponding to **all available data**, **indexed by taxi_type, date, and pickup_place**
3) **Stores it as Parquet** and supports **local + S3** inputs
4) **Discards rows with fewer than 50 rides** (total count across hours). 

Uses modular utilities, handles large files via partitioning and parallelization
with production-style code (logging, error handling, tests). Note that while
this project suggested using `Dask` dataframes to perform parallelization, we chose
to utilize `ProcessPoolExecutor` instead due to the nature of the computations
needed to perform pivots and other dataframe related operations for `Pandas`. Thus,
all related partition optimization code is proof of concept and tested, but ultimately
unused in the final pipeline run.

**S3 Storage**:  
Project reads data from:
```
s3://dsc291-ucsd/taxi/Dataset/
```
and writes to a specified output directory (local or S3).   
Our production run final parquet table is stored to:
```
s3://dsc291-taxi/taxi-output2/taxi_wide_table.parquet
```

## Project Structure

```
DSC-291-AWS/
├── pivot_and_bootstrap/                    # Main pipeline implementation
│   ├── bucket_policy.json                  # S3 bucket policy example for anon public read/write access
│   ├── HOMEWORK_ASSIGNMENT_1.md            # Instructions for this project assignment
│   ├── io_utils.py                         # Core io operations for local/s3 file management
│   ├── log.txt                             # Example log output if you pipe output '> log.txt'
│   ├── partition_optimization.py           # Proof of concept S3 partition handling for Dask
│   ├── performance.md                      # Performance summary of our AWS run
│   ├── pivot_all_files.py                  # Main processing script
│   ├── pivot_utils.py                      # Core pivot operations
│   ├── README.md                           # This file
│   ├── requirements.txt                    # Required dependencies
│   ├── taxi_zones.shp/shx                  # Shape files needed for pickup data calculation
│   ├── test_pivot_date_location_hour.py    # Proof of concept testing operations file
│   ├── test.bat/sh                         # Easy OS specific running scripts
│   ├── test.ipynb                          # Early exploration of data and test outputs
└── .gitignore                              # git ignore file for data and pycache files
```

## Setup
Project runs on `python3`. Install required dependencies via pip:
```bash
pip install -r requirements.txt
```

## Usage
```bash
python taxi_pivot_pipeline.py \
    --input-dir <INPUT_DIR> \
    --output-dir <OUTPUT_DIR> \
    [OPTIONS]
```

### Required Arguments
| Argument | Description |
|----------|-------------|
| `--input-dir` | Path to the input directory containing taxi data files (local or S3) |
| `--outpt-dir` | Path to the directory where output and intermediary files will be written (local or S3)

### Optional Arguments
| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--min-rides` | int | `50` | Minimum number of rides needed in a pickup location to be considered for processing |
| `--workers` | int | `1` | Number of worker procceses to use
| `--partition-size` | str | `None` | Target partition size to use (proof of concept, e.g. `500MB`, `1GB`)
| `--skip-partition-optimization` | flag | off | Skip partition optimization step (proof of concept)
| `--keep-intermediate` | flag | off | Preserve intermediate files instead of deleting them 

### Example
```bash
python3 pivot_all_files.py \
    --input-dir "s3://dsc291-ucsd/taxi/Dataset/" \
    --output-dir "s3://dsc291-taxi/taxi-output2/" \
    --workers 16 2>&1 | tee output-mem.txt
```
`2>&1 | tee output-mem.txt` allows all outputted logging information to show
both in run console and piped to the file `output-mem.txt`
