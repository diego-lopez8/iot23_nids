"""
This file fetches the IoT-23 labeled dataset for processing and analysis.
"""

import logging
import requests
import tarfile
import io
from tqdm import tqdm
# TODO: figure out a way for all files to include this without pasting it everywhere 
logging.basicConfig(level=logging.INFO, filename="fetch_dataset.log") 

def download_file(url, filename="zeek.tar.gz"):
    """
    Download file from URL to the specified filename
    
    Parameters
    ----------
    filename: str
        Name of the downloaded file. Defaults to "zeek.tar.gz".
    """
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        total_size_in_bytes = int(r.headers.get('content-length', 0))
        block_size = 8192
        progress_bar = tqdm(total=total_size_in_bytes, unit='iB', unit_scale=True)
        with open(filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=block_size):
                progress_bar.update(len(chunk))
                f.write(chunk)
        progress_bar.close()

def extract_tarfile(filename="zeek.tar.gz"):
    """
    Extract contents of a tar.gz file
    
    Parameters
    ----------
    filename: str
        Name of the file to be extracted. Defaults to "zeek.tar.gz".
    """
    if filename.endswith("tar.gz"):
        with tarfile.open(filename, "r:gz") as tar:
            tar.extractall()
            print(f"Extracted all contents of {filename}")

if __name__ == "__main__":
    logging.info("Beginning dataset download")
    download_file("https://mcfp.felk.cvut.cz/publicDatasets/IoT-23-Dataset/iot_23_datasets_small.tar.gz")
    logging.info("Download Complete!")
    logging.info("Extracting tarball")
    extract_tarfile()
    logging.info("Extraction Complete!")