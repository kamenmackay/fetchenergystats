import pandas as pd
import argparse
import yaml
import logging
from datetime import datetime
import requests
import os

logging.basicConfig(level=logging.INFO)


def load_credentials():
    with open("creds.yaml", "r") as file:
        return yaml.safe_load(file)


def save_data_to_csv(file_name_prefix, start_date, end_date, data, outputdirectory):
    file_name = f"{file_name_prefix}{start_date}-{end_date}.csv"
    file_name = file_name.replace("-", "_")
    output_path = os.path.join(outputdirectory, file_name)
    logging.info(f"CSV output saved to {output_path}")
    return data.to_csv(output_path)


def save_data_to_parquet(file_name_prefix, start_date, end_date, data, outputdirectory):
    file_name = f"{file_name_prefix}{start_date}-{end_date}.parquet"
    file_name = file_name.replace("-", "_")
    output_path = os.path.join(outputdirectory, file_name)
    logging.info(f"Parquet output saved to {output_path}")
    return data.to_parquet(output_path)


def fetch_givenergy_flows(creds, start_date, end_date):
    """Fetches Givenergy stats"""
    url = "https://api.givenergy.cloud/v1/inverter/EA2302G468/energy-flows"
    token = creds["givenergy"]["token"]
    headers = {
        "Authorization": token,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    logging.info(start_date)
    start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
    end_datetime = datetime.strptime(end_date, "%Y-%m-%d")

    # Now format them back to strings if needed
    start_time = start_datetime.strftime("%Y-%m-%d")
    end_time = end_datetime.strftime("%Y-%m-%d")
    payload = {
        "start_time": start_time,
        "end_time": end_time,
        "grouping": 0,
        "types": [0, 1, 2, 3, 4, 5, 6],
    }

    session = requests.Session()
    response = session.request("POST", url, headers=headers, json=payload)
    if len(response.text) > 15:
        try:
            df = pd.DataFrame(response.json())
            logging.info(df)
            unpacked_df = pd.json_normalize(df["data"])
            column_mapping = {
                "data.0": "PV_to_Home",
                "data.1": "PV_to_Battery",
                "data.2": "PV_to_Grid",
                "data.3": "Grid_to_Home",
                "data.4": "Grid_to_Battery",
                "data.5": "Battery_to_Home",
                "data.6": "Battery_to_Grid",
            }
            unpacked_df.rename(columns=column_mapping, inplace=True)
            unpacked_df["Total_from_Grid"] = (
                unpacked_df["Grid_to_Home"] + unpacked_df["Grid_to_Battery"]
            )
            unpacked_df["Total_to_Grid"] = (
                unpacked_df["PV_to_Grid"] + unpacked_df["Battery_to_Grid"]
            )
            unpacked_df = unpacked_df.sort_values(by="start_time").round(4)
            logging.info(unpacked_df)
            return unpacked_df
        except Exception as e:
            logging.error("something wrong with dataframe")
            logging.error(e.with_traceback, e.args)


def fetch_octo_stats(creds, start_date, end_date):
    """Fetches Octopus Energy stats"""
    octopusApiKey = creds["octopus"]["api_key"]
    outgoingMPAN = creds["octopus"]["mpan"]
    outgoingMeterSerial = creds["octopus"]["meter_serial"]

    params = {
        "period_from": f"{start_date}",
        "period_to": f"{end_date}",
        "page_size": 2000,
    }
    #  'group_by': 'day'}
    endpoint = f"https://api.octopus.energy/v1/electricity-meter-points/{outgoingMPAN}/meters/{outgoingMeterSerial}/consumption/"

    session = requests.Session()
    response = session.get(endpoint, auth=(octopusApiKey, ""), params=params)

    if response.status_code == 200:
        logging.info(f"Consumption data for meter point: {endpoint}")

        df = pd.DataFrame(response.json())
        unpacked_df = pd.json_normalize(df["results"])
        unpacked_df = unpacked_df.rename(columns={"consumption": "octoconsumption"})

        output_df = (
            unpacked_df[["interval_start", "interval_end", "octoconsumption"]]
            .sort_values(by="interval_start")
            .round(4)
        )
        output_df.set_index = "start_time"
        logging.info(f"Consumption for {outgoingMPAN}")
        return output_df
    else:
        logging.error(response.text)


def main():
    creds = load_credentials()

    parser = argparse.ArgumentParser(description="Fetch Energy Data")
    parser.add_argument("start_date", help="Start date in format YYYY-MM-DD")
    parser.add_argument("end_date", help="End date in format YYYY-MM-DD")
    parser.add_argument(
        "--source",
        choices=["givenergy", "octopus"],
        required=True,
        help="Data source: givenergy or octopus",
    )
    parser.add_argument("--directory", required=True, help="Output directory")
    parser.add_argument(
        "--format", choices=["csv", "parquet"], required=True, help="Output format"
    )

    args = parser.parse_args()
    outputdir = args.directory

    if args.source == "givenergy":
        data = fetch_givenergy_flows(creds, args.start_date, args.end_date)
        file_name_prefix = "givenergy"
        if args.format == "csv":
            save_data_to_csv(
                file_name_prefix, args.start_date, args.end_date, data, outputdir
            )
        else:
            save_data_to_parquet(
                file_name_prefix, args.start_date, args.end_date, data, outputdir
            )
    elif args.source == "octopus":
        data = fetch_octo_stats(creds, args.start_date, args.end_date)
        file_name_prefix = "octopus"
        if args.format == "csv":
            save_data_to_csv(
                file_name_prefix, args.start_date, args.end_date, data, outputdir
            )
        else:
            save_data_to_parquet(
                file_name_prefix, args.start_date, args.end_date, data, outputdir
            )


if __name__ == "__main__":
    main()
