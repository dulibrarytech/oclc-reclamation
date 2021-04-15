import os
import xml.etree.ElementTree as ET

# Create sets:
# - mms_ids_already_processed set (empty set)
# - alma_records_with_current_oclc_num (from CSV file, if provided; otherwise,
#   empty set)

for file in os.listdir(directory_with_xml_files):
    if not file.endswith('.xml'):
        continue

    # Get XML file as ET.Element

    # for record in ET.Element:
        # Extract MMS ID from 001 field

        # Check if MMS ID is a member of mms_ids_already_processed set
        # If so, continue (i.e. skip to next record)

        # Add MMS ID to mms_ids_already_processed set

        # Extract all OCLC numbers from 035 $a fields and add to list

        # If list length > 1, append MMS ID and OCLC numbers to
        # master_list_records_with_errors CSV file (an error message within the
        # CSV file probably isn't necessary for this case).

        # Check if MMS ID is a member of alma_records_with_current_oclc_num set
        # - If so, append MMS ID and OCLC number to
        #   master_list_records_with_current_oclc_num CSV file
        # - If not, append MMS ID and OCLC number to
        #   master_list_records_with_potentially_old_oclc_num CSV file
