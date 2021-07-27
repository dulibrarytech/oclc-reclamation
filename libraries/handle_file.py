import libraries.record
import logging
import logging.config
from csv import reader
from typing import Set

logging.config.fileConfig('logging.conf', disable_existing_loggers=False)
logger = logging.getLogger(__name__)


def csv_column_to_set(path_to_csv: str, target_set: Set[str],
        col_num: int, keep_leading_zeros: bool) -> None:
    if path_to_csv is not None:
        with open(path_to_csv, mode='r', newline='') as file:
            file_reader = reader(file)
            for row in file_reader:
                value = row[col_num]

                if isinstance(value, str):
                    value = value.strip()
                    if not keep_leading_zeros and value.isdigit():
                        value = libraries.record.remove_leading_zeros(value)

                target_set.add(value)
