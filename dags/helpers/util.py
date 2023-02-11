import os


def create_dir(directory: str):
    if not os.path.exists(directory):
        os.makedirs(directory)
