import os
import requests
import xml.etree.ElementTree as ET
from dotenv import load_dotenv

load_dotenv()

API_URL = os.getenv('API_URL')
mms_id = '991013441369702766'
params = {'view': 'full'}
API_KEY = os.getenv('API_KEY')
headers = {'Authorization': 'apikey ' + API_KEY}

response = requests.get(API_URL + mms_id, params=params, headers=headers,
    timeout=45)

print(response)
print('Request URL:', response.url)
print('Status:', response.status_code)
print('Raise for status:', response.raise_for_status())
print('Encoding:', response.encoding)

root = ET.fromstring(response.text)

record_element = root.find('./record')

# Get index of first 035 element
first_035_element_index = list(record_element).index(
    record_element.find('./datafield[@tag="035"]')
)

# To get the index of last 035 element, this might be helpful:
# https://stackoverflow.com/questions/35371607/parsing-xml-find-last-element-with-matching-attributes
#
# However, I think it's going to be tricky because there are non-035 children as well.
# So even if the 035 elements are grouped together, finding the index of the last such element
# is still difficult.
#
# SOLUTION: If we can assume that <record>'s children are in ascending order, then you can find the
# index of the first element > 035 and insert into that index.

print('Index of first 035 element:', first_035_element_index)

print('\nFirst 035 element:')
ET.dump(record_element[first_035_element_index])

# Create new 035 element with OCLC number
new_035_element = ET.Element('datafield')
new_035_element.set('ind1', ' ')
new_035_element.set('ind2', ' ')
new_035_element.set('tag', '035')
sub_element = ET.SubElement(new_035_element, 'subfield')
sub_element.set('code', 'a')
sub_element.text = '(OCoLC)ocm12345678 '

# Insert new 035 element into XML
record_element.insert(first_035_element_index, new_035_element)

print('\nFirst 035 element after insert:')
ET.dump(record_element[first_035_element_index])

print('\nOriginal record:')
print(response.text)

# Send PUT request
headers['Content-Type'] = 'application/xml'
payload = ET.tostring(root, encoding='UTF-8')
print('Type:', type(payload))
print('Payload:', payload)
put_response = requests.put(API_URL + mms_id, headers=headers,
    data=payload, timeout=45)

print('\nPUT reponse:', put_response)
print('Request URL:', put_response.url)
print('Status:', put_response.status_code)
print('Raise for status:', put_response.raise_for_status())
print('Encoding:', put_response.encoding)
print('\nPUT response body:')
print(put_response.text)
