import pathlib

import requests
import pandas as pd
import fastparquet as fp


def download_csv():
    base_url = "https://storage.googleapis.com/eo_best_practices_2023/EOenergy"

    for m in range(1, 13):
        # file_name = f"LST_analysis_above_solar_irradiation_threshold_2024_{m:02}.csv"
        file_name = f"lst_DNI_monthly_data_2024_{m:02}.csv"
        url = f"{base_url}/{file_name}"
        r = requests.get(url)

        if r.status_code != 200:
            continue

        with open(f"source_data/lst_DNI_monthly_data_2024/{file_name}", "wt") as f:
            f.write(r.text)


def csv_to_parquet():
    dfs = []

    for file_name in pathlib.Path("source_data/LST_analysis_above_solar_irradiation_threshold_2024").glob("*.csv"):
        dfs.append(pd.read_csv(
            file_name,
            index_col=[0],
            parse_dates=[0],
            date_format="%Y-%m-%d %H:%M:%S",
            header=0,
            names=["date", "DNI", "LST"]
        ))

    df = pd.concat(dfs).sort_index()
    fp.write("output/LST_analysis_above_solar_irradiation_threshold_2024.parquet", df)


if __name__ == "__main__":
    # download_csv()
    csv_to_parquet()
