"""
Author: Diego Troy Lopez
Take as input the folder for IoT23 data, and sample each scenario down to a maximum of 
1 million flows per category per scenario. If a category in any scenario has less than 1 million results, 
it takes the entirety of the data for that category. 
"""
import pandas as pd 
import os
from zat.log_to_dataframe import LogToDataFrame
from zat.dataframe_to_matrix import DataFrameToMatrix
from pathlib import Path
import argparse
import logging
# TODO: figure out a way for all files to include this without pasting it everywhere 
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s') 
# Args parser
parser = argparse.ArgumentParser(description="This script takes the downloaded and extracted IoT23 Dataset and samples the scenarios for ML tasks.")
parser.add_argument("-d", "--data-dir", type=str, help="Path to data directory.", required=True)
args = parser.parse_args()
# Global values
SAMPLE_SIZE_PERP_LABEL=1_000_000
RANDOM_STATE=42
# Global objects
log_to_df = LogToDataFrame()
bro_df_complete = pd.DataFrame()

def find_raw_data(directory):
    """
    Crawls the given directory for data with the `.labeled` extension, used by IoT23 labeled Zeek logs
    """
    path = Path(directory)
    for file in path.rglob('*'):
        if file.is_file() and file.__str__().endswith(".labeled"):
            logging.info(f"Processing scenario {file.__str__()}")
            process_scenario(file.__str__())

def process_scenario(path):
    """
    Processes the labeled Zeek logs with pandas and ZAT. Concatenates them to a global dataframe
    """
    global bro_df_complete
    global log_to_df
    temp_df = log_to_df.create_dataframe(path, aggressive_category=False, ts_index=False)
    temp_df[['tunnel_parents', 'label', 'detailed_label']] =  temp_df['tunnel_parents   label   detailed-label'].str.split('   ', expand=True)
    temp_df = temp_df.drop(columns=['tunnel_parents   label   detailed-label'])
    for unique_label in temp_df['detailed_label'].unique():
        unique_label_temp_df = temp_df[temp_df['detailed_label'] == unique_label]
        if (len(unique_label_temp_df) < SAMPLE_SIZE_PERP_LABEL):
            bro_df_complete = pd.concat([bro_df_complete, unique_label_temp_df])
        else:
            unique_label_temp_df = unique_label_temp_df.sample(n=SAMPLE_SIZE_PERP_LABEL, random_state=RANDOM_STATE, replace=False)
            bro_df_complete      = pd.concat([bro_df_complete, unique_label_temp_df])

def clean_dataset(bro_df_complete):
    """
    Cleans NaNs, encodes categorical features, drops irrelevant features in the global dataframe. 
    """
    # fix broken "benign" label
    bro_df_complete['label'] = bro_df_complete['label'].replace({"benign": "Benign"})
    # drop ipv6
    bro_df_complete = bro_df_complete[bro_df_complete['id.orig_h'].str.contains("::") == False]
    # drop unused columns
    bro_df_complete = bro_df_complete.drop(columns=["uid", "tunnel_parents", "local_resp", "local_orig"])
    # handle NaNs
    bro_df_complete['orig_bytes'] = bro_df_complete['orig_bytes'].fillna(0)
    bro_df_complete['resp_bytes'] = bro_df_complete['resp_bytes'].fillna(0)
    bro_df_complete['duration'] = bro_df_complete['duration'].dt.total_seconds().fillna(0)
    # Encode History Variable
    bro_df_complete = bro_df_complete[bro_df_complete['history'].notna()]
    # break out history to separate columns
    for l in ['S','h','A', 'D', 'a', 'd', 'F', 'f']:
        bro_df_complete[f'history_has_{l}'] = bro_df_complete['history'].apply(lambda x: 1 if l in x else 0)
    bro_df_complete = bro_df_complete.drop(columns='history')
    # IP feature extractions
    bro_df_complete['is_destination_broadcast'] = bro_df_complete['id.resp_h'].apply(lambda x: 1 if "255" in x[-3:] else 0) # create broadcast variable
    # encode service
    bro_df_complete['service'] = bro_df_complete['service'].fillna("-")
    # OHE the relevant categorical data
    bro_df_complete = pd.get_dummies(data=bro_df_complete, columns=['conn_state', "proto", "service"])
    return bro_df_complete

def main():
    # TODO: add a intermediary non-cleaned dataframe?
    find_raw_data(args.data_dir)
    bro_df_complete = clean_dataset(bro_df_complete)
    bro_df_complete.to_parquet(f"{args.data_dir}/processed/bro_df_complete.parquet")

if __name__ == "__main__":
    main()