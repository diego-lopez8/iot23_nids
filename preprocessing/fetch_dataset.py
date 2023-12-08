"""
Author: Diego Lopez
This file fetches the IoT-23 labeled dataset for processing and analysis.
"""

import logging
import requests
import tarfile
import io
from tqdm import tqdm
import argparse
# TODO: figure out a way for all files to include this without pasting it everywhere 
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s') 
# Args parser
parser = argparse.ArgumentParser(description="This script downloads the IoT-23 dataset from the web. ")
parser.add_argument("-d", "--data-dir", type=str, help="Path to data directory.", required=True)
args = parser.parse_args()

def download_file(url, data_dir):
    """
    Download file from URL to the specified filename
    
    Parameters
    ----------
    filename: str
        Name of the downloaded file. Defaults to "zeek.tar.gz".
    """
    data_dir = data_dir + "/raw/"
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        total_size_in_bytes = int(r.headers.get('content-length', 0))
        block_size = 8192
        progress_bar = tqdm(total=total_size_in_bytes, unit='iB', unit_scale=True)
        with open(data_dir + "iot_23_datasets_small.tar.gz", 'wb') as f:
            for chunk in r.iter_content(chunk_size=block_size):
                progress_bar.update(len(chunk))
                f.write(chunk)
        progress_bar.close()

def extract_tarfile(data_dir):
    """
    Extract contents of a tar.gz file
    
    Parameters
    ----------
    filename: str
        Name of the file to be extracted. Defaults to "zeek.tar.gz".
    """
    filename = data_dir + "/raw/" + "iot_23_datasets_small.tar.gz"
    if filename.endswith("tar.gz"):
        with tarfile.open(filename, "r:gz") as tar:
            tar.extractall(data_dir + "/raw")
            print(f"Extracted all contents of {filename}")

def main():
    logging.info("Beginning dataset download")
    download_file("https://mcfp.felk.cvut.cz/publicDatasets/IoT-23-Dataset/iot_23_datasets_small.tar.gz", args.data_dir)
    logging.info("Download Complete!")
    logging.info("Extracting tarball")
    extract_tarfile(args.data_dir)
    logging.info("Extraction Complete!")

if __name__ == "__main__":
    main()