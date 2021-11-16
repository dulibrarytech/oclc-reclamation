# OCLC Reclamation

## Table of Contents

- [README](#readme)
  - [Background](#background)
  - [Licenses](#licenses)
  - [Local Environment Setup](#local-environment-setup)
  - [Using the Scripts](#using-the-scripts)
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

#### Update Alma Records

**Script name:** `update_alma_records.py`

**Usage notes:**
```
usage: update_alma_records.py [option] input_file

positional arguments:
  input_file     the name and path of the input file, which must be in either
                 CSV (.csv) or Excel (.xlsx or .xls) format (e.g.
                 inputs/update_alma_records/filename.csv)

example: python update_alma_records.py inputs/update_alma_records/filename.csv
```

For required format of input file, see either:
- `inputs/update_alma_records/example.csv`
- `inputs/update_alma_records/example.xlsx`

**Description:** Updates Alma records to have the corresponding OCLC number.

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

### Maintainers

@scottsalvaggio

### Acknowledgements

@freyesdulib, @jrynhart, @kimpham54

## Contact

Ways to get in touch:

* Contact the Digital Infrastructure & Technology Coordinator at [University of Denver, Library Technology Services](https://library.du.edu/contact/department-directory.html)
* Create an issue in this repository
