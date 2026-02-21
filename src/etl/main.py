import pandas as pd
import logging
from pathlib import Path

# Configure logging properly
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def run():
    logging.info("ETL Started")

    # Get project root directory (3 levels up from this file)
    BASE_DIR = Path(__file__).resolve().parent.parent.parent

    input_path = BASE_DIR / "input.csv"
    output_path = BASE_DIR / "output.parquet"

    logging.info(f"Reading file from: {input_path}")

    # Read CSV
    df = pd.read_csv(input_path)

    # Convert to datetime
    df["pickup_time"] = pd.to_datetime(df["pickup_time"])
    df["dropoff_time"] = pd.to_datetime(df["dropoff_time"])

    # Create derived column
    df["trip_duration_minutes"] = (
        (df["dropoff_time"] - df["pickup_time"]).dt.total_seconds() / 60
    )

    # Drop null values
    df = df.dropna()

    # Write to parquet
    df.to_parquet(output_path, index=False)

    logging.info(f"Output written to: {output_path}")
    logging.info("ETL Completed Successfully")


if __name__ == "__main__":
    run()