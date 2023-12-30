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
    path = Path(directory)
    for file in path.rglob('*'):
        if file.is_file() and file.__str__().endswith(".labeled"):
            logging.info(f"Processing scenario {file.__str__()}")
            process_scenario(file.__str__())

def process_scenario(path):
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

def main():
    find_raw_data(args.data_dir)
    bro_df_complete.to_parquet(f"{args.data_dir}/processed/bro_df_complete.parquet")

if __name__ == "__main__":
    main()