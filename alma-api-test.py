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

API_URL = os.getenv('API_URL')
API_KEY = os.getenv('API_KEY')
headers = {'Authorization': f'apikey {API_KEY}'}
params = {'view': 'full'}

def get_alma_record(mms_id):
    """GET record based on MMS ID. Return root element of parsed XML tree."""

    try:
        response = requests.get(f'{API_URL}{mms_id}', params=params, headers=headers,
            timeout=45)
        logger.debug(f'GET reponse: {response}')
        logger.debug(f'Request URL: {response.url}')
        logger.debug(f'Status: {response.status_code}')
        logger.debug(f'Encoding: {response.encoding}')
        response.raise_for_status()
    except HTTPError as http_err:
        logger.exception(f'HTTP error occurred: {http_err}')
        return -1
    except Exception as err:
        logger.exception(f'Error occurred: {err}')
        return -1

    # Pretty-print XML response
    xml_as_pretty_printed_str = \
        minidom.parseString(response.text).toprettyxml(indent='  ',
        encoding='UTF-8')
    print('\nOriginal record:')
    print(str(xml_as_pretty_printed_str, 'utf-8'))

    # Create XML file
    with open(f'xml/{mms_id}_original.xml', 'wb') as file:
        file.write(xml_as_pretty_printed_str)

    return ET.fromstring(response.text)

def update_alma_record(mms_id, oclc_num):
    """Insert OCLC number into Alma record."""

    # Make sure OCLC number contains numbers only
    if not oclc_num.isdigit():
        print(f'ERROR: Invalid OCLC number: "{oclc_num}" must contain only ' \
            f'digits.')
        return

    # Create full OCLC number string based on length of oclc_num
    full_oclc_num = '(OCoLC)'
    oclc_num_len = len(oclc_num)
    if oclc_num_len == 8:
        full_oclc_num += f'ocm{oclc_num} '
    elif oclc_num_len == 9:
        full_oclc_num += f'ocn{oclc_num}'
    elif oclc_num_len > 9:
        full_oclc_num += f'on{oclc_num}'
    else:
        print(f'ERROR: Invalid OCLC number: "{oclc_num}" contains ' \
            f'{oclc_num_len} digits. To be valid, it must contain 8 or more ' \
            f'digits.')
        return

    print('Full OCLC number:', full_oclc_num)

    root = get_alma_record(mms_id)

    if root == -1:
        return -1

    record_element = root.find('./record')

    # Get index of first 035 element
    first_035_element_index = list(record_element).index(
        record_element.find('./datafield[@tag="035"]')
    )
    print('\nIndex of first 035 element:', first_035_element_index)

    print('\nFirst 035 element:')
    ET.dump(record_element[first_035_element_index])

    # Check if first 035 element already has an OCLC number
    print('\nSubfield:', record_element[first_035_element_index][0].text)
    if record_element[first_035_element_index][0].text.startswith('(OCoLC)'):
        print('\nSkipping record because it already has an OCLC number.')
        return

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

    print('\nFirst 035 element after insert:')
    ET.dump(record_element[first_035_element_index])

    # Send PUT request
    headers['Content-Type'] = 'application/xml'
    payload = ET.tostring(root, encoding='UTF-8')

    try:
        put_response = requests.put(f'{API_URL}{mms_id}', headers=headers,
            data=payload, timeout=45)
        logger.debug(f'PUT reponse: {put_response}')
        logger.debug(f'Request URL: {put_response.url}')
        logger.debug(f'Status: {put_response.status_code}')
        logger.debug(f'Encoding: {put_response.encoding}')
        put_response.raise_for_status()
    except HTTPError as http_err:
        logger.exception(f'HTTP error occurred: {http_err}')
        return -1
    except Exception as err:
        logger.exception(f'Error occurred: {err}')
        return -1

    # Pretty-print XML response
    xml_as_pretty_printed_str = \
        minidom.parseString(put_response.text).toprettyxml(indent='  ',
        encoding='UTF-8')
    print('\nModified record:')
    print(str(xml_as_pretty_printed_str, 'utf-8'))

    # create XML file
    with open(f'xml/{mms_id}_modified.xml', 'wb') as file:
        file.write(xml_as_pretty_printed_str)

# Convert excel file into pandas DataFrame
data = pd.read_excel('xlsx/alma-test.xlsx', 'Sheet1', engine='openpyxl',
    dtype={'MMS ID': 'str', 'OCLC Number': 'str'})

# Loop over rows in DataFrame and update the corresponding Alma record
for index, row in data.iterrows():
    update_alma_record(row['MMS ID'], row['OCLC Number'])
