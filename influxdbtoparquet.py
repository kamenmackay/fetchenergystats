import sys

sys.path.append("/home/kmackay/scripts")
import argparse
import pandas as pd
from influxdb import DataFrameClient

import structlog_config
import os
import structlog


def main(start_date, end_date):
    log = structlog.get_logger()
    measurement_list = ["Wh", "W", "°C", "V"]
    # measurement_list = ["°C"]
    master_df = None

    client = DataFrameClient(host="192.168.8.150", port=8086)
    client.switch_database("hass")
    result = client.query("SHOW MEASUREMENTS")
    # measurement_list = [ measurement['name'] for measurement in result.get_points() if re.search('givtcp', measurement['name'], re.IGNORECASE) ]
    # measurement_list = [measurement['name'] for measurement in result.get_points() if re.search('givtcp', measurement['name'], re.IGNORECASE)]
    print(measurement_list)
    for measurement in measurement_list:
        log.info("Grabbing measurement", measurement=measurement)
        if measurement.lower().startswith("select"):
            log.warning(
                "Skipping problematic measurement name", measurement=measurement
            )
            continue
        query = (
            f"SELECT * FROM \"{measurement}\" WHERE time >= '{start_date}T00:00:00Z' "
            f"AND time <= '{end_date}T23:59:59Z' AND \"entity_id\" =~ /givtcp/"
        )
        df = None
        try:
            df = client.query(query).get(measurement)
            if df is not None and not df.empty:
                # Explicitly specify the format or let pandas infer it
                df["time"] = pd.to_datetime(
                    df["time"], format="ISO8601", errors="coerce"
                )
                df["measurement"] = measurement  # Add measurement name as a new column
                if master_df is None:
                    master_df = df
                else:
                    master_df = pd.concat([master_df, df])
        except Exception as e:
            log.error(
                "An error occurred while querying data", exception=str(e), exc_info=True
            )
            pass

        if df is not None:
            df["measurement"] = measurement  # Add measurement name as a new column

            if master_df is None:
                master_df = df
            else:
                master_df = pd.concat([master_df, df])

    parquet_filename = "givenergy_" + start_date.replace("-", "") + ".gz.parquet"
    log.info(f"Sorting index for {start_date}")
    master_df.sort_index(inplace=True, ascending=True)
    master_df.to_parquet(parquet_filename, compression="gzip")
    log.info("Saved to Parquet file", filename=parquet_filename)


if __name__ == "__main__":
    script_name = os.path.basename(__file__).split(".")[0]
    structlog_config.configure_logging(script_name)

    parser = argparse.ArgumentParser(
        description="Specify start date for data fetching."
    )
    parser.add_argument(
        "--start_date", required=True, help="Start date in YYYY-MM-DD format"
    )
    parser.add_argument(
        "--end_date", required=True, help="End date in YYYY-MM-DD format"
    )
    args = parser.parse_args()
    main(args.start_date, args.end_date)
