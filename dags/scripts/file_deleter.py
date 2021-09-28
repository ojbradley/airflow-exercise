import os
import logging

# Clean up any unwanted files post-write based on their extension
def remove_files_recursively(path, extension: list()):
    print("Starting file cleanup...")
    for root, dirs, files in os.walk(path):
        for currentFile in files:            
            print(f'''Checking {currentFile} for matching extension...''')
            if currentFile.lower().endswith(extension):
                print(f'''Deleting {currentFile}''')
                os.remove(os.path.join(root, currentFile))