from pathlib import Path

import fastparquet as fp
import numpy as np
import requests
import pandas as pd
import zarr
from zarr import group
from zarr.storage import LocalStore


def download_csv(base_file_name: str):
    base_url = "https://storage.googleapis.com/eo_best_practices_2023/EOenergy"

    for m in range(1, 13):
        file_name = f"{base_file_name}_{m:02}.csv"

        output_file_path = Path(f"source_data/{base_file_name}/{file_name}")
        if output_file_path.exists():
            print(f"File {output_file_path} already exists. Skipping.")
            continue

        url = f"{base_url}/{file_name}"
        r = requests.get(url)

        if r.status_code != 200:
            print(f"Request failed with status code {r.status_code}. Skipping.")
            continue

        with open(output_file_path, "wt") as f:
            print(f"Writing to {output_file_path}")
            f.write(r.text)


def csv_to_parquet(base_file_name: str):
    dfs = []

    for file_name in Path(f"source_data/{base_file_name}").glob("*.csv"):
        print(f"Reading {file_name}")
        dfs.append(pd.read_csv(
            file_name,
            index_col=[0],
            parse_dates=[0],
            date_format="%Y-%m-%d %H:%M:%S",
            header=0,
            names=["date", "DNI", "LST"]
        ))

    df = pd.concat(dfs).sort_index()
    output_file_name = f"output/{base_file_name}.parquet"
    print(f"Writing to {output_file_name}")
    fp.write(output_file_name, df)


def csv_to_zarr(base_file_name_a: str, base_file_name_b: str):
    """Stores all CSV data as a Zarr array, separated by year and chunked by week.
    Each sample type is a separate array. If this is too granular, we can lump them together.
    """
    dfs = []

    for file_name in Path(f"source_data/{base_file_name_a}").glob("*.csv"):
        print(f"Reading {file_name}")
        dfs.append(pd.read_csv(
            file_name,
            index_col=[0],
            parse_dates=[0],
            date_format="%Y-%m-%d %H:%M:%S",
            header=0,
            names=["date", "DNI", "LST"]
        ))

    df = pd.concat(dfs).sort_index()

    dfs = []

    for file_name in Path(f"source_data/{base_file_name_b}").glob("*.csv"):
        print(f"Reading {file_name}")
        dfs.append(pd.read_csv(
            file_name,
            index_col=[0],
            parse_dates=[0],
            date_format="%Y-%m-%d %H:%M:%S",
            header=0,
            names=["date", "DNI_threshold", "LST"]
        ))

    df2 = pd.concat(dfs).sort_index()
    df["DNI_threshold"] = df2["DNI_threshold"]
    timestamps = df.index.values.astype('datetime64[ns]')

    store = LocalStore(f"output/{base_file_name_a}.zarr")
    root = group(store=store, overwrite=True, zarr_version=3)
    year_group = root.create_group("year")
    g2024 = year_group.create_group("2024")
    chunk_size = 336  # Samples per week.
    g2024.create_array("timestamp", data=timestamps, chunks=(chunk_size,))
    g2024.create_array("DNI", data=df['DNI'].values, chunks=(chunk_size,))
    g2024.create_array("DNI_threshold", data=df['DNI_threshold'].values, chunks=(chunk_size,))
    g2024.create_array("LST", data=df['LST'].values, chunks=(chunk_size,))
    store.close()



def main():
    # base_file_name = "lst_DNI_monthly_data_2024"
    # download_csv(base_file_name)
    # csv_to_parquet(base_file_name)

    # base_file_name = "LST_analysis_above_solar_irradiation_threshold_2024"
    # download_csv(base_file_name)
    # csv_to_parquet(base_file_name)

    csv_to_zarr("lst_DNI_monthly_data_2024", "LST_analysis_above_solar_irradiation_threshold_2024")


if __name__ == "__main__":
    main()
