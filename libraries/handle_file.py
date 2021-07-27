import libraries.record
import logging
import logging.config
from csv import reader
from typing import Set

logging.config.fileConfig('logging.conf', disable_existing_loggers=False)
logger = logging.getLogger(__name__)


def csv_column_to_set(path_to_csv: str, target_set: Set[str], col_num: int,
        keep_leading_zeros: bool) -> None:
    """Adds values from the specified column of the CSV file to the target set.

    Parameters
    ----------
    path_to_csv: str
        The name and path of the CSV file
    target_set: Set[str]
        The set to be populated
    col_num: int
        The desired column number (zero-indexed) of the CSV file
    keep_leading_zeros: bool
        True if leading zeros should be retained for each value (if applicable);
        False, otherwise (i.e. leading zeros should be removed from each value)
    """
    if path_to_csv is not None:
        with open(path_to_csv, mode='r', newline='') as file:
            file_reader = reader(file)
            for row_index, row in enumerate(file_reader, start=1):
                if col_num < len(row):
                    value = row[col_num]

                    if isinstance(value, str):
                        value = value.strip()
                        if value.isdigit():
                            if not keep_leading_zeros:
                                value = \
                                    libraries.record.remove_leading_zeros(value)
                        else:
                            logger.warning(f'{path_to_csv}, row #{row_index} '
                                f'contains a value with at least one non-digit '
                                f'character: {value}')
                    else:
                        logger.warning(f'{path_to_csv}, row #{row_index} '
                            f'contains the value "{value}", which is not a '
                            f'string, but rather of type: {type(value)}')

                    target_set.add(value)
