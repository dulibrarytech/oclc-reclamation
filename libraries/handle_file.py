import libraries.record
import logging
import logging.config
import csv
from typing import Set

logging.config.fileConfig('logging.conf', disable_existing_loggers=False)
logger = logging.getLogger(__name__)


def csv_column_to_set(path_to_csv: str, target_set: Set[str], col_num: int,
        keep_leading_zeros: bool) -> None:
    """Adds values from the specified column of the CSV file to the target set.

    Parameters
    ----------
    path_to_csv: str
        The name and path of the CSV file containing the source data
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
            file_reader = csv.reader(file)
            for row_index, row in enumerate(file_reader, start=1):
                if col_num < len(row):
                    value = row[col_num]

                    if isinstance(value, str):
                        value = value.strip()
                        value = \
                            libraries.record.remove_surrounding_quotes(value)
                        value = \
                            libraries.record.remove_oclc_org_code_prefix(value)
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


def set_to_csv(source_set: Set[str], set_name: str, csv_writer: csv.writer,
        col_heading: str) -> None:
    """Add all values from the source set to the specified CSV file.

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
    logger.debug(f'{set_name} = {source_set}')
    logger.debug(f'type({set_name}) = {type(source_set)}')
    logger.debug(f'len({set_name}) = {len(source_set)}\n')

    if csv_writer is not None and len(source_set) > 0:
        csv_writer.writerow([col_heading])
        for value in source_set:
            csv_writer.writerow([value])
