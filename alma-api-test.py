import os
import pandas as pd
import requests
import xml.etree.ElementTree as ET
from xml.dom import minidom
from dotenv import load_dotenv

load_dotenv()

API_URL = os.getenv('API_URL')
mms_id = '991017143999702766'
oclc_num = '12345678 ' # <-- has 'ocm' prefix'; '123456789' <-- has 'ocn' prefix
params = {'view': 'full'}
API_KEY = os.getenv('API_KEY')
headers = {'Authorization': 'apikey ' + API_KEY}

def get_alma_record(mms_id):
    """GET record based on MMS ID. Return root element of parsed XML tree."""
    response = requests.get(API_URL + mms_id, params=params, headers=headers,
        timeout=45)

    print('\nGET reponse:', response)
    print('Request URL:', response.url)
    print('Status:', response.status_code)
    print('Raise for status:', response.raise_for_status())
    print('Encoding:', response.encoding)

    # Pretty-print XML response
    xml_as_pretty_printed_str = \
        minidom.parseString(response.text).toprettyxml(indent='  ',
        encoding='UTF-8')
    print('\nOriginal record:')
    print(str(xml_as_pretty_printed_str, 'utf-8'))

    # Create XML file
    with open('xml/' + mms_id + '_original.xml', 'wb') as file:
        file.write(xml_as_pretty_printed_str)

    return ET.fromstring(response.text)

def update_alma_record(mms_id, oclc_num):
    """Insert OCLC number into Alma record."""

    print('Type of mms_id:', type(mms_id))
    print('Type of oclc_num:', type(oclc_num))

    # Make sure OCLC number contains numbers only
    if not oclc_num.isdigit():
        print('ERROR: Invalid OCLC number: ' + oclc_num +
            ' must contain only digits.')
        return

    root = get_alma_record(mms_id)
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

    # Create full OCLC number string based on length of oclc_num
    full_oclc_num = '(OCoLC)'
    oclc_num_len = len(oclc_num)
    if oclc_num_len == 8:
        full_oclc_num += 'ocm' + oclc_num + ' '
    elif oclc_num_len == 9:
        full_oclc_num += 'ocn' + oclc_num
    elif oclc_num_len > 9:
        full_oclc_num += 'on' + oclc_num
    else:
        print('ERROR: Invalid OCLC number. The OCLC number is: ' + oclc_num +
            ', which contains ' + str(oclc_num_len) + ' digits. To be valid, ' +
            'it should contain 8 or more digits.')
        return

    print('Full OCLC number:', full_oclc_num)
    sub_element.text = full_oclc_num

    # Insert new 035 element into XML
    record_element.insert(first_035_element_index, new_035_element)

    print('\nFirst 035 element after insert:')
    ET.dump(record_element[first_035_element_index])

    # Send PUT request
    headers['Content-Type'] = 'application/xml'
    payload = ET.tostring(root, encoding='UTF-8')
    put_response = requests.put(API_URL + mms_id, headers=headers,
        data=payload, timeout=45)

    print('\nPUT reponse:', put_response)
    print('Request URL:', put_response.url)
    print('Status:', put_response.status_code)
    print('Raise for status:', put_response.raise_for_status())
    print('Encoding:', put_response.encoding)

    # Pretty-print XML response
    xml_as_pretty_printed_str = \
        minidom.parseString(put_response.text).toprettyxml(indent='  ',
        encoding='UTF-8')
    print('\nModified record:')
    print(str(xml_as_pretty_printed_str, 'utf-8'))

    # create XML file
    with open('xml/' + mms_id + '_modified.xml', 'wb') as file:
        file.write(xml_as_pretty_printed_str)

# Convert excel file into pandas DataFrame
data = pd.read_excel('xlsx/alma-test.xlsx', 'Sheet1', engine='openpyxl',
    dtype={'MMS ID': 'str', 'OCLC Number': 'str'})

# Loop over rows in DataFrame and update the corresponding Alma record
for index, row in data.iterrows():
    update_alma_record(row['MMS ID'], row['OCLC Number'])
