import os
import tarfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm

def extract_tarfile(tar_path, output_path):
    with tarfile.open(tar_path, "r") as tar:
        tar.extractall(path=output_path)

def process_tarfile(tar_file, base_directory, output_directory):
    tar_path = os.path.join(base_directory, tar_file)
    folder_name = os.path.splitext(tar_file)[0]
    output_path = os.path.join(output_directory, folder_name)
    os.makedirs(output_path, exist_ok=True)
    extract_tarfile(tar_path, output_path)
    return f"Extracted tar file: {tar_path} to {output_path}"

def main():
    # Define the directory where your tar files are located
    base_directory = "/path/to/your/tar/files"
    # Define the directory where you want to save the extracted folders
    output_directory = "/path/to/save/extracted/folders"
    
    # Ensure the output directory exists
    os.makedirs(output_directory, exist_ok=True)

    print(f"Saving in {output_directory}")

    # List all .tar files in the base directory
    tar_files = [f for f in os.listdir(base_directory) if f.endswith('.tar')]
    
    # Initialize the progress bar
    with tqdm(total=len(tar_files), desc="Extracting tar files", unit="file") as pbar:
        with ProcessPoolExecutor(max_workers=4) as executor:
            futures = {executor.submit(process_tarfile, tar_file, base_directory, output_directory): tar_file for tar_file in tar_files}
            
            for future in as_completed(futures):
                try:
                    result = future.result()
                    #print(result)
                except Exception as exc:
                    print(f"An error occurred: {exc}")
                finally:
                    pbar.update(1)  # Update the progress bar

if __name__ == "__main__":
    main()
