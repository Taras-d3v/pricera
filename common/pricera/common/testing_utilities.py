import os
import gzip
from typing import Union, List


def load_file_from_sub_folder(
    filename: str,
    test_file_path: str,
    sub_folder: str = "test_cases",
) -> Union[str, List[dict]]:

    # Get the directory containing the test file
    test_file_dir = os.path.dirname(test_file_path)

    # Build the correct path: test_file_dir + sub_folder + filename
    file_path = os.path.join(test_file_dir, sub_folder, filename)

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    if filename.endswith(".gz"):
        with gzip.open(file_path, mode="rt", encoding="utf-8") as gz_file:
            return gz_file.read()
    else:
        with open(file_path, "r", encoding="utf-8") as f:
            return f.read()
