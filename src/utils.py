import json
import os
import logging
import colorlog
import pandas as pd
import re


def read_json_files_from_folder(folder_path: str) -> list[dict]:
    return [
        read_json_file(file_path)
        for file_path in get_all_file_paths(folder_path)
        if file_path.endswith(".json")
    ]


def read_json_file(file_path):
    try:
        with open(file_path, "r") as file:
            data = json.load(file)
        return data
    except FileNotFoundError:
        raise FileNotFoundError(f"File '{file_path}' not found.")
    except json.JSONDecodeError as e:
        raise ValueError(f"{file_path} is not a json: {e}")


def get_all_file_paths(folder_path: str) -> list[str]:
    """
    Get all file paths from a folder and its subfolders.

    Args:
    - folder_path (str): Path to the folder.

    Returns:
    - List of file paths.
    """
    file_paths = []
    for root, _, files in os.walk(folder_path):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            file_paths.append(file_path)
    if not file_paths:
        raise FileNotFoundError(f"No files found in the directory: {folder_path}")
    return file_paths


def get_class_methods(class_obj):
    # Get all attributes of the class
    class_attributes = dir(class_obj)

    # Filter out only the methods (functions)
    methods = [attr for attr in class_attributes if callable(getattr(class_obj, attr))]

    return methods


def get_logger(name):
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # Create a ColorFormatter
    formatter = colorlog.ColoredFormatter(
        "%(asctime)s - %(log_color)s%(levelname)s%(reset)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        log_colors={
            "DEBUG": "cyan",
            "INFO": "green",
            "WARNING": "yellow",
            "ERROR": "red",
            "CRITICAL": "bold_red",
        },
        reset=True,
        style="%",
    )

    # Create console handler and set level to debug
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    # Add the ColorFormatter to the console handler
    console_handler.setFormatter(formatter)

    # Add console handler to logger
    logger.addHandler(console_handler)

    return logger


# this is not required can repurpose this to filter dict and provide cols like that
def give_table_context(df: pd.DataFrame) -> dict:
    return {
        "df": df,
        "columns": [convert_camel_to_title(col) for col in df.columns.tolist()],
    }


def convert_camel_to_title(camel_str):
    heading = re.sub(r"(?<!^)(?=[A-Z])", " ", camel_str)
    heading = heading.title()
    return heading
