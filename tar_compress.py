# Define the directory where your folders are located
#base_directory = "/media/data/Datasets/imagenet21k_resized/imagenet21k_train"
# Define the directory where you want to save the tar files
#output_directory = "/media/data/Datasets/imagenet21k_resized/imagenet21k_train_tar"

import os
import tarfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm

def create_tarfile(folder_path, output_path):
    with tarfile.open(output_path, "w") as tar:  # "w" mode is used for uncompressed tar files
        tar.add(folder_path, arcname=os.path.basename(folder_path))

def process_folder(folder, base_directory, output_directory):
    folder_path = os.path.join(base_directory, folder)
    output_path = os.path.join(output_directory, f"{folder}.tar")
    create_tarfile(folder_path, output_path)
    return f"Created tar file: {output_path}"

def main():
    # Define the directory where your folders are located
    base_directory = "/media/data2/imagenet21k_masks/outputs_imagenet21k"
    # Define the directory where you want to save the tar files
    output_directory = "/media/data2/imagenet21k_masks/outputs_imagenet21k_tar"
    
    # Ensure the output directory exists
    os.makedirs(output_directory, exist_ok=True)

    print(f"Saving in {output_directory}")

    # List all directories in the base directory
    folders = [f for f in os.listdir(base_directory) if os.path.isdir(os.path.join(base_directory, f))]
    
    # Initialize the progress bar
    with tqdm(total=len(folders), desc="Creating tar files", unit="file") as pbar:
        with ProcessPoolExecutor(max_workers=4) as executor:
            futures = {executor.submit(process_folder, folder, base_directory, output_directory): folder for folder in folders}
            
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
