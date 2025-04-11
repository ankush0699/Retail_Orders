import os
import subprocess
import zipfile
import logging

def download_from_kaggle(dataset: str, download_path: str):
    logging.info("Downloading full dataset from Kaggle")
    
    # Ensure the download folder exists
    os.makedirs(download_path, exist_ok=True)

    # Name of the downloaded zip file
    zip_name = f"{dataset.split('/')[-1]}.zip"
    zip_path = os.path.join(download_path, zip_name)

    # Remove existing zip file if it exists
    if os.path.exists(zip_path):
        logging.info("Removing existing dataset zip file: %s", zip_path)
        os.remove(zip_path)

    try:
        # Run Kaggle CLI to download dataset
        subprocess.run([
            "kaggle", "datasets", "download",
            "-d", dataset,
            "-p", download_path
        ], check=True)

        # Unzip the dataset
        logging.info("Unzipping downloaded dataset...")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(download_path)

        # Optionally remove the zip file after extraction
        os.remove(zip_path)
        logging.info("Deleted zip file: %s", zip_path)

        logging.info("Download and unzip completed.")
    
    except subprocess.CalledProcessError as e:
        logging.error("Kaggle dataset download failed: %s", e)
        raise RuntimeError(f"Kaggle download failed: {e}")
