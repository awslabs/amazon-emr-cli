import os
from pathlib import Path
from shutil import copytree, ignore_patterns
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


def copy_template(source: str, target_dir: str):
    """
    Copies the entire `source` directory to `target_dir`.
    """
    source = os.path.abspath(Path(__file__).parent.parent / "templates" / source)
    copytree(
        source, target_dir, dirs_exist_ok=True, ignore=ignore_patterns("__pycache__")
    )
