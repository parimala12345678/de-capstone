# DE Capstone Project – Week 1

## Project Overview
This project demonstrates a basic ETL (Extract, Transform, Load) pipeline built using Python.

## Dataset
The dataset contains:
- trip_id
- pickup_time
- dropoff_time
- fare

## ETL Process

### Extract
- Imported required libraries (pandas, logging, pyarrow).
- Loaded the CSV file using pandas.read_csv().
- Verified the schema and column names.

### Transform
The following transformations were performed:

- Converted pickup_time and dropoff_time columns into datetime format using pd.to_datetime().
- Created a new derived column:

  trip_duration_minutes

  This column calculates the trip duration in minutes using:

### Load
Saved processed data into output.parquet

## Tools Used
- Python
- Pandas
- PyArrow
- Git
- GitHub

## Status
ETL pipeline successfully completed and pushed to GitHub.
