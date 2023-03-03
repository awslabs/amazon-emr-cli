import os
from typing import List
from urllib.parse import urlparse


def console_log(message):
    print(f"[emr-cli]: {message}")


def find_files(directory, excluded_dirs=[], search=None) -> List[str]:
    files = []
    for root, dirs, filenames in os.walk(directory):
        dirs[:] = [d for d in dirs if d not in excluded_dirs]
        for filename in filenames:
            if search is None or filename == search or filename.endswith(search):
                files.append(os.path.join(root, filename))
    return files


def parse_bucket_uri(uri: str) -> List[str]:
    result = urlparse(uri, allow_fragments=False)
    return [result.netloc, result.path.strip("/")]


def mkdir(path: str):
    try:
        os.mkdir(path)
    except FileExistsError:
        pass
