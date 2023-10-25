import gzip
import os
import re
import sys
from pathlib import Path
from shutil import copyfile, copytree, ignore_patterns
from typing import TYPE_CHECKING, Dict, List
from urllib.parse import urlparse

from rich.progress import Progress, TotalFileSizeColumn

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
else:
    S3Client = object


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
    if sys.version_info.major == 3 and sys.version_info.minor == 7:
        py37_copytree(source, target_dir, ignore=ignore_patterns("__pycache__"))
    else:
        copytree(
            source,
            target_dir,
            dirs_exist_ok=True,
            ignore=ignore_patterns("__pycache__"),
        )


def py37_copytree(src, dest, ignore=None):
    """
    A Python3 3.7 version of shutils.copytree since `dirs_exist_ok` was introduced in 3.8
    """
    if os.path.isdir(src):
        if not os.path.isdir(dest):
            os.makedirs(dest)
        files = os.listdir(src)
        if ignore is not None:
            ignored = ignore(src, files)
        else:
            ignored = set()
        for f in files:
            if f not in ignored:
                py37_copytree(os.path.join(src, f), os.path.join(dest, f), ignore)
    else:
        copyfile(src, dest)


def validate_build_target(name: str) -> bool:
    """
    Grep the local Dockerfile for the desired target, raise an exception if it's not found
    """
    r = None
    search_term = f"FROM .* AS {name}$"
    with open("Dockerfile", "r") as file:
        for line in file:
            r = re.search(search_term, line, flags=re.IGNORECASE)
            if r:
                return True
    if not r:
        console_log(f"ERR: Target `{name}` not found in Dockerfile.")
        console_log(
            "ERR: Try creating a new dockerfile with the `emr init --dockerfile .` command."
        )
        sys.exit(1)

    return False


def print_s3_gz(client: S3Client, s3_uri: str):
    """
    Downloads and decompresses a gzip file from S3 and prints the logs to stdout.
    """
    bucket, key = parse_bucket_uri(s3_uri)
    gz = client.get_object(Bucket=bucket, Key=key)
    with gzip.open(gz["Body"]) as data:
        print(data.read().decode())


class PrettyUploader:
    def __init__(
        self,
        s3_client: S3Client,
        bucket: str,
        src_target: Dict[str, str],
    ):
        self._s3_client = s3_client
        self._bucket = bucket
        self._src_target = src_target
        self._totalsize = sum(
            [float(os.path.getsize(filename)) for filename in self._src_target.keys()]
        )
        self._seensize = 0
        self._progress = Progress(
            *Progress.get_default_columns(), TotalFileSizeColumn()
        )
        self._task = self._progress.add_task("Uploading...", total=self._totalsize)

    def run(self):
        with self._progress:
            for src, target in self._src_target.items():
                self._s3_client.upload_file(src, self._bucket, target, Callback=self)

    def __call__(self, bytes_amount):
        self._progress.update(self._task, advance=bytes_amount)
