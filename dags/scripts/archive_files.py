import os

def archive_files(input_dir: str, archive_dir: str):
    
    files_to_archive = os.listdir(input_dir)

    for file in files_to_archive:
        os.rename(input_dir + file, archive_dir + file)