# OCLC Reclamation

## Table of Contents

- [README](#readme)
  - [Background](#background)
  - [Licenses](#licenses)
  - [Local Environment Setup](#local-environment-setup)
  - [Using the Scripts](#using-the-scripts)
    - [`update_alma_records.py`](#update_alma_recordspy)
    - [`extract_record_identifiers.py`](#extract_record_identifierspy)
  - [Maintainers](#maintainers)
  - [Acknowledgements](#acknowledgements)
- [Contact](#contact)

## README

### Background

Python scripts to reconcile a library's local holdings in Ex Libris Alma with
OCLC's WorldCat database.

The current test script (`alma-api-test.py`) takes an Excel file
(`alma-test.xlsx`) and, for each row, adds the corresponding OCLC Number to the
specified Alma record (indicated by the MMS ID).

You will need an Ex Libris Developer Network account and an API key (see [Alma
API documentation](https://developers.exlibrisgroup.com/alma/apis/) for more
details).

### Licenses

[Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).

All other content is released under [CC-BY-4.0](https://creativecommons.org/licenses/by/4.0/).

### Local Environment Setup

- Go into `oclc-reclamation` folder (i.e. root folder of repository)
- Create and activate virtual environment:
  - `python -m venv venv`
  - `source venv/bin/activate`
- Install python dependencies:
  - `pip install -r requirements.txt`
- Add `.env` file to root folder (you can copy `.env-example`)
  - To use the `update_alma_records.py` script, initialize these variables:
    - `ALMA_BIBS_API_URL`
      - If you're in North America, use
      `https://api-na.hosted.exlibrisgroup.com/almaws/v1/bibs/`
      - If not,
      [look here](https://developers.exlibrisgroup.com/alma/apis/#calling) for
      the base URL for your geographic region
    - `ALMA_API_KEY`
      - [Instructions for creating an API key](https://developers.exlibrisgroup.com/alma/apis/#using)
  - To use the `process_worldcat_records.py` script, initialize these variables:
    - `WORLDCAT_METADATA_API_KEY`
    - `WORLDCAT_METADATA_API_SECRET`
      - If you don't already have an OCLC web service key (aka WSKey), you'll
      need to [request one](https://www.oclc.org/developer/develop/authentication/how-to-request-a-wskey.en.html).
      - When filling out the request form, be sure to choose "WorldCat Metadata
      API" under Services. Note that, at the time of writing, the WorldCat
      Metadata API service was only available when requesting a Production
      WSKey. If your Production WSKey request is approved and you do not
      also receive a Sandbox WSKey for the WorldCat Metadata API service, reach
      out to OCLC to ask for one, as this is very helpful when testing the
      scripts.

### Using the Scripts

#### `update_alma_records.py`

##### Usage notes

```
usage: update_alma_records.py [option] input_file

positional arguments:
  input_file     the name and path of the input file, which must be in either
                 CSV (.csv) or Excel (.xlsx or .xls) format
                 (e.g. inputs/update_alma_records/filename.csv)

example: python update_alma_records.py inputs/update_alma_records/filename.csv
```

For required format of input file, see either:
- `inputs/update_alma_records/example.csv`
- `inputs/update_alma_records/example.xlsx`

##### Description

Updates Alma records to have the corresponding OCLC number.

For each row in the input file, the corresponding OCLC number is added to
the specified Alma record (indicated by the MMS ID), unless the Alma record
already contains that OCLC number. If the Alma record contains non-matching
OCLC numbers in an 035 field (in the subfield $a), those OCLC numbers are
moved to the 019 field (as long as they are valid).

When processing each Alma record:
- The original record is saved in XML format as:
`outputs/update_alma_records/xml/{mms_id}_original.xml`
- If the record is updated, then it is added to
`outputs/update_alma_records/records_updated.csv` and the modified Alma record
is saved in XML format as:
`outputs/update_alma_records/xml/{mms_id}_modified.xml`
- If the record is not updated because it already has the current OCLC number,
then it is added to:
`outputs/update_alma_records/records_with_no_update_needed.csv`
- If an error is encountered, then the record is added to:
`outputs/update_alma_records/records_with_errors.csv`

See `main()` function's docstring (within `update_alma_records.py`) to learn
about:
- how OCLC numbers are recognized and extracted
- what constitutes a valid OCLC number for the purposes of this script
- how invalid OCLC numbers are handled

---

#### `extract_record_identifiers.py`

##### Usage notes

```
usage: extract_record_identifiers.py [option] directory_with_xml_files [alma_records_with_current_oclc_num]

positional arguments:
  directory_with_xml_files
                        the path to the directory containing the XML files to process
  alma_records_with_current_oclc_num
                        the name and path of the CSV file containing the MMS IDs
                        of all Alma records with a current OCLC number
                        (e.g. inputs/extract_record_identifiers/alma_records_with_current_oclc_num.csv)

example: python extract_record_identifiers.py inputs/extract_record_identifiers/xml_files_to_extract_from/ inputs/extract_record_identifiers/alma_records_with_current_oclc_num.csv
```

To create/populate the `directory_with_xml_files`, you will need to export the
XML files from Alma. Here's one approach:
- Create the following directory for these XML files:
`inputs/extract_record_identifiers/xml_files_to_extract_from/`
- Create sets in Alma that contain the records whose holdings should be set in
WorldCat. Begin each set name with the same prefix, e.g. "OCLC Reclamation" to
facilitate easy retrieval of all sets.
- Export these sets as XML files:
  - You can do this by [running a job on each set][https://knowledge.exlibrisgroup.com/Alma/Product_Documentation/010Alma_Online_Help_(English)/050Administration/070Managing_Jobs/020Manual_Jobs_on_Defined_Sets].
  - For Select Job to Run, choose "Export Bibliographic Records".
  - Select the set you want to export.
  - Choose "MARC21 Bibliographic" as the Output Format and "XML" as the Physical
  Format.
  - If you want the XML file to be downloadable by others in your institution,
  choose "Institution" for Export Into Folder. Otherwise, leave it as "Private".
  - When the job is complete, download the XML file to the desired directory,
  e.g. `inputs/extract_record_identifiers/xml_files_to_extract_from/`.

For required format of `alma_records_with_current_oclc_num` input file, see:
- `example_file_for_alma_records_with_current_oclc_num.csv`

##### Description

For each XML file in the specified directory, the MMS ID and OCLC Number(s)
from each Alma record are extracted and appended to the appropriate
`outputs/extract_record_identifiers/master_list_records` CSV file.

When processing each Alma record:
- If an error is encountered, then the record is added to:
`outputs/extract_record_identifiers/master_list_records_with_errors.csv`
- If the record's MMS ID appears in the optional
`alma_records_with_current_oclc_num` input file, then the record is added to:
`outputs/extract_record_identifiers/master_list_records_with_current_oclc_num.csv`
- Otherwise, the record is added to:
`outputs/extract_record_identifiers/master_list_records_with_potentially_old_oclc_num.csv`

### Maintainers

@scottsalvaggio

### Acknowledgements

@freyesdulib, @jrynhart, @kimpham54

## Contact

Ways to get in touch:

* Contact the Digital Infrastructure & Technology Coordinator at [University of Denver, Library Technology Services](https://library.du.edu/contact/department-directory.html)
* Create an issue in this repository
