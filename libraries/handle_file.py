import csv
import dotenv
import libraries.record
import logging
import os
from typing import Set

dotenv_file = dotenv.find_dotenv()
dotenv.load_dotenv(dotenv_file)

logger = logging.getLogger(__name__)

col_headings = {'MMS ID', 'OCLC Number', '035$a'}


def csv_column_to_set(path_to_csv: str, target_set: Set[str], col_num: int,
        keep_leading_zeros: bool) -> None:
    """Adds values from the specified column of the CSV file to the target set.

    Note that path_to_csv can be a CSV file (.csv) or a text file (.txt) with
    CSV formatting.

    Parameters
    ----------
    path_to_csv: str
        The name and path of the CSV or text file containing the source data
    target_set: Set[str]
        The set to be populated
    col_num: int
        The specific column number (zero-indexed) to add
    keep_leading_zeros: bool
        True if leading zeros should be retained for each value (if applicable);
        False, otherwise (i.e. leading zeros should be removed from each value)

    Raises
    ------
    ValueError
        If the path_to_csv argument does not end with '.csv' or '.txt'
    """

    if path_to_csv is None:
        return
    elif not path_to_csv.endswith(('.csv', '.txt')):
        raise ValueError(f'Invalid file format ({path_to_csv}). Must be one of '
            f'the following file formats: CSV (.csv) or text (.txt).')

    with open(path_to_csv, mode='r', newline='') as file:
        file_reader = csv.reader(file)
        for row_index, row in enumerate(file_reader, start=1):
            if col_num < len(row):
                value = row[col_num]

                # Skip column heading, if applicable
                if row_index == 1 and value in col_headings:
                    continue

                if isinstance(value, str):
                    value = value.strip().strip("\"'")
                    value = libraries.record.remove_oclc_org_code_prefix(value)
                    if value.isdigit():
                        if not keep_leading_zeros:
                            value = \
                                libraries.record.remove_leading_zeros(value)
                    else:
                        logger.warning(f'{path_to_csv}, row #{row_index} '
                            f'contains a value with at least one non-digit '
                            f'character: {value}\n')
                else:
                    logger.warning(f'{path_to_csv}, row #{row_index} '
                        f'contains the value "{value}", which is not a string, '
                        f'but rather of type: {type(value)}\n')

                target_set.add(value)


def set_env_var(key_to_set: str, value_to_set: str) -> None:
    """Adds or updates the specified environment variable (in OS and .env file).

    Parameters
    ----------
    key_to_set: str
        The name of the environment variable to set
    value_to_set: str
        The new value of the environment variable
    """

    os.environ[key_to_set] = value_to_set
    dotenv.set_key(dotenv_file, key_to_set, value_to_set)


def set_to_csv(source_set: Set[str], set_name: str, csv_writer: csv.writer,
        col_heading: str) -> None:
    """Adds all values from the source set to the specified CSV writer object.

    Parameters
    ----------
    source_set: Set[str]
        The set containing the source data
    set_name: str
        The name of the source set
    csv_writer: csv.writer
        The target CSV file's writer object
    col_heading: str
        The desired column heading for the CSV file
    """

    # logger.debug(f'{set_name} = {source_set}')
    logger.debug(f'len({set_name}) = {len(source_set)}\n')

    if csv_writer is not None and len(source_set) > 0:
        csv_writer.writerow([col_heading])
        for value in source_set:
            csv_writer.writerow([value])
