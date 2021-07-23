import logging
import logging.config
from csv import reader
from typing import Set

logging.config.fileConfig('logging.conf', disable_existing_loggers=False)
logger = logging.getLogger(__name__)


def csv_column_to_set(path_to_csv: str, target_set: Set[str],
        col_num: int) -> None:
    if path_to_csv is not None:
        with open(path_to_csv, mode='r', newline='') as file:
            file_reader = reader(file)
            for row in file_reader:
                target_set.add(row[col_num])
