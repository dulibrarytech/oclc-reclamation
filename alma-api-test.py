import logging
import logging.config
import os
import pandas as pd
import requests
import xml.etree.ElementTree as ET
from dotenv import load_dotenv
from requests.exceptions import HTTPError
from xml.dom import minidom

load_dotenv()

logging.config.fileConfig('logging.conf', disable_existing_loggers=False)
logger = logging.getLogger(__name__)

alma_bibs_api_url = os.environ["ALMA_BIBS_API_URL"]
headers = {'Authorization': f'apikey {os.environ["ALMA_API_KEY"]}'}
params = {'view': 'full'}

def get_alma_record(mms_id):
    """GET record based on MMS ID. Return root element of parsed XML tree."""

    response = requests.get(
        f'{alma_bibs_api_url}{mms_id}',
        params=params,
        headers=headers,
        timeout=45)
    logger.debug(f'GET reponse: {response}')
    logger.debug(f'Request URL: {response.url}')
    logger.debug(f'Status: {response.status_code}')
    logger.debug(f'Encoding: {response.encoding}')
    response.raise_for_status()

    # Pretty-print XML response
    xml_as_pretty_printed_str = \
        minidom.parseString(response.text).toprettyxml(indent='  ',
        encoding='UTF-8')
    logger.debug(f'Original record:\n' \
        f'{xml_as_pretty_printed_str.decode("UTF-8")}')

    # Create XML file
    with open(f'xml/{mms_id}_original.xml', 'wb') as file:
        file.write(xml_as_pretty_printed_str)

    # Return root element of XML tree
    return ET.fromstring(response.text)

def update_alma_record(mms_id, oclc_num):
    """Insert OCLC number into Alma record."""

    logger.debug(f'Attempting to update MMS ID "{mms_id}"...')

    # Make sure MMS ID contains numbers only.
    # Don't validate the length because "The MMS ID can be 8 to 19 digits long
    # (with the first two digits referring to the record type and the last four
    # digits referring to a unique identifier for the institution)".
    # Source: https://knowledge.exlibrisgroup.com/Alma/Product_Documentation/010Alma_Online_Help_(English)/120Alma_Glossary
    assert mms_id.isdigit(), f'Invalid MMS ID: "{mms_id}" must ' \
        f'contain only digits.'

    # Make sure OCLC number contains numbers only and has at least 8 digits
    assert oclc_num.isdigit(), f'Invalid OCLC number: "{oclc_num}" must ' \
        f'contain only digits.'

    oclc_num_len = len(oclc_num)
    assert oclc_num_len >= 8, f'Invalid OCLC number: "{oclc_num}" contains ' \
        f'{oclc_num_len} digits. To be valid, it must contain 8 or more digits.'

    # Create full OCLC number string based on length of oclc_num
    full_oclc_num = '(OCoLC)'
    if oclc_num_len == 8:
        full_oclc_num += f'ocm{oclc_num} '
    elif oclc_num_len == 9:
        full_oclc_num += f'ocn{oclc_num}'
    else:
        full_oclc_num += f'on{oclc_num}'

    logger.debug(f'Full OCLC number: {full_oclc_num}')

    # Access XML elements of Alma record
    root = get_alma_record(mms_id)
    record_element = root.find('./record')

    # Get index of first 035 element
    first_035_element_index = list(record_element).index(
        record_element.find('./datafield[@tag="035"]')
    )
    logger.debug(f'Index of first 035 element: {first_035_element_index}')

    first_035_element_as_str = \
        ET.tostring(record_element[first_035_element_index], encoding="unicode")
    logger.debug(f'First 035 element:\n{first_035_element_as_str}')

    # Check if first 035 element already has an OCLC number
    # TO DO: If so, compare OCLC numbers and update/move if different?
    # TO DO: Consider checking all 035 elements for (OCoLC) prefix (instead of
    # just the first element). Note that you will have to do this for the
    # script that updates the OCLC number.
    assert not record_element[first_035_element_index][0].text.startswith(
        '(OCoLC)'), \
        f'This record already has an OCLC number: ' \
        f'"{record_element[first_035_element_index][0].text}".'

    # Create new 035 element with OCLC number
    new_035_element = ET.Element('datafield')
    new_035_element.set('ind1', ' ')
    new_035_element.set('ind2', ' ')
    new_035_element.set('tag', '035')
    sub_element = ET.SubElement(new_035_element, 'subfield')
    sub_element.set('code', 'a')
    sub_element.text = full_oclc_num

    # Insert new 035 element into XML
    record_element.insert(first_035_element_index, new_035_element)

    first_035_element_as_str = \
        ET.tostring(record_element[first_035_element_index], encoding="unicode")
    logger.debug(f'First 035 element after insert:\n{first_035_element_as_str}')

    # Send PUT request
    headers['Content-Type'] = 'application/xml'
    payload = ET.tostring(root, encoding='UTF-8')

    put_response = requests.put(
        f'{alma_bibs_api_url}{mms_id}',
        headers=headers,
        data=payload,
        timeout=45)
    logger.debug(f'PUT reponse: {put_response}')
    logger.debug(f'Request URL: {put_response.url}')
    logger.debug(f'Status: {put_response.status_code}')
    logger.debug(f'Encoding: {put_response.encoding}')
    put_response.raise_for_status()

    # Pretty-print XML response
    xml_as_pretty_printed_str = \
        minidom.parseString(put_response.text).toprettyxml(indent='  ',
        encoding='UTF-8')
    logger.debug(f'Modified record:\n' \
        f'{xml_as_pretty_printed_str.decode("UTF-8")}')

    # Create XML file
    with open(f'xml/{mms_id}_modified.xml', 'wb') as file:
        file.write(xml_as_pretty_printed_str)

    logger.debug(f'MMS ID "{mms_id}" has been updated.')

# Convert excel file into pandas DataFrame
data = pd.read_excel('xlsx/alma-test.xlsx', 'Sheet1', engine='openpyxl',
    dtype={'MMS ID': 'str', 'OCLC Number': 'str'})

# Loop over rows in DataFrame and update the corresponding Alma record
num_records_updated = 0
for index, row in data.iterrows():
    try:
        update_alma_record(row['MMS ID'], row['OCLC Number'])
        num_records_updated += 1
    except AssertionError as assert_err:
        logger.exception(f'An assertion error occurred when processing ' \
            f'MMS ID "{row["MMS ID"]}": {assert_err}')
    except HTTPError as http_err:
        logger.exception(f'An HTTP error occurred when processing MMS ID ' \
            f'"{row["MMS ID"]}": {http_err}')
    except Exception as err:
        logger.exception(f'An error occurred when processing MMS ID ' \
            f'"{row["MMS ID"]}": {err}')
print(f'\nEnd of script. {num_records_updated} of {len(data.index)} ' \
    f'records updated.')
