# OCLC Reclamation

## Table of Contents

- [README](#readme)
  - [Background](#background)
    - [Alma API Key](#alma-api-key)
    - [OCLC Web Service Key](#oclc-web-service-key)
  - [Licenses](#licenses)
  - [Local Environment Setup](#local-environment-setup)
  - [Using the Scripts](#using-the-scripts)
    - [Preventing your system from going to sleep while the scripts are running](#preventing-your-system-from-going-to-sleep-while-the-scripts-are-running)
    - [`search_worldcat.py`](#search_worldcatpy)
    - [`update_alma_records.py`](#update_alma_recordspy)
    - [`extract_record_identifiers.py`](#extract_record_identifierspy)
    - [`process_worldcat_records.py`](#process_worldcat_recordspy)
    - [`compare_alma_to_worldcat.py`](#compare_alma_to_worldcatpy)
  - [Reclamation Process Overview](#reclamation-process-overview)
  - [Maintainers](#maintainers)
  - [Acknowledgements](#acknowledgements)
- [Contact](#contact)

## README

### Background

Python scripts to reconcile a library's local holdings in Ex Libris Alma with
OCLC's WorldCat database.

Some of the scripts require an API key:

#### Alma API Key

For the `update_alma_records.py` script, you will need an Ex Libris Developer
Network account and an API key (see the
[Alma API documentation](https://developers.exlibrisgroup.com/alma/apis/) for
more details).

Once logged into the Ex Libris Developer Network, follow these
[instructions for creating an API key](https://developers.exlibrisgroup.com/alma/apis/#using).

You should create both a Production and a Sandbox API key. That way, you can use
your Sandbox API key when testing the `update_alma_records.py` script.

When you click the "Add Permission" button, choose "Bibs" for Area, either
"Production" or "Sandbox" for Env (depending on which key you're creating), and
"Read/write" for Permissions.

#### OCLC Web Service Key

For the `search_worldcat.py` and `process_worldcat_records.py` scripts, you will
need an OCLC web service key (aka WSKey) with access to the WorldCat Metadata
API service. Follow these
[instructions to request one](https://www.oclc.org/developer/develop/authentication/how-to-request-a-wskey.en.html).

When filling out the request form, be sure to choose "WorldCat Metadata API"
under Services. Note that, at the time of writing, the WorldCat Metadata API
service was only available when requesting a Production WSKey. If your
Production WSKey request is approved and you do not also receive a Sandbox WSKey
for the WorldCat Metadata API service, reach out to OCLC to ask for one, as this
is very helpful when testing the scripts.

##### Important note about WorldCat Metadata API sandbox testing

Even when using your Sandbox WSKey, you should still be careful when using the
WorldCat Metadata API, as
[explained here](https://www.oclc.org/developer/api/oclc-apis/worldcat-metadata-api/sandbox-testing.en.html).

For example, when running the `process_worldcat_records.py` script with the
`set_holding` or `unset_holding` operation, your
*Sandbox WSKey can update your institution's actual holdings in WorldCat.* To
avoid this, make sure your `input_file` consists exclusively of Test Sandbox
Records. (Your WSKey approval email from OCLC should include the OCLC numbers
for these Test Sandbox Records.)

In contrast, it is safe to use real WorldCat records when testing this script's
`get_current_oclc_number` operation because it does not update the records.

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
    - `ALMA_API_BASE_URL`
      - If you're in North America, use
      `https://api-na.hosted.exlibrisgroup.com`
      - If not,
      [look here](https://developers.exlibrisgroup.com/alma/apis/#calling) for
      the base URL for your geographic region
    - `ALMA_API_KEY`
      - See [Alma API Key](#alma-api-key) section for how to request one.
  - To use the `search_worldcat.py` and `process_worldcat_records.py` scripts,
  initialize these variables:
    - `OCLC_INSTITUTION_SYMBOL`
    - `WORLDCAT_METADATA_API_KEY`
    - `WORLDCAT_METADATA_API_SECRET`
      - Your OCLC WSKey for the WorldCat Metadata API service will include both
      a key and secret. See [OCLC Web Service Key](#oclc-web-service-key)
      section for how to request one.

### Using the Scripts

#### Preventing your system from going to sleep while the scripts are running

These scripts can take some time to complete, especially if they are processing
many records. So be sure to disable your system's sleep settings prior to
running any of the scripts. Otherwise, the scripts could get interrupted during
execution.

For Mac users: You can prevent idle sleep while a script is running by using the
`caffeinate` command-line tool. From the Terminal, just prepend `caffeinate -i`
to the desired script command. For example:
```
caffeinate -i python update_alma_records.py inputs/update_alma_records/filename.csv
```
With this approach, you won't have to adjust your sleep settings.

#### `search_worldcat.py`

##### Usage notes

```
usage: search_worldcat.py [-h] [-v] [--search_my_library_holdings_first] input_file

positional arguments:
  input_file     the name and path of the input file, which must be in either
                 CSV (.csv) or Excel (.xlsx or .xls) format (e.g.
                 inputs/search_worldcat/filename.csv)

optional arguments:
  -h, --help     show this help message and exit
  -v, --version  show program's version number and exit
  --search_my_library_holdings_first
                 whether to first search WorldCat for your library's holdings.
                 - Use this option if you want to search in the following order:
                   1) Search with "held by" filter.
                   2) If there are no WorldCat search results held by your library,
                      then search without "held by" filter.
                 - Without this option, the default search order is as follows:
                   1) Search without "held by" filter.
                   2) If there is more than one WorldCat search result, then search with
                      "held by" filter to narrow down the results.

examples:
  python search_worldcat.py inputs/search_worldcat/filename.csv
  python search_worldcat.py --search_my_library_holdings_first inputs/search_worldcat/filename.csv
```

For required format of input file, see either:
- `inputs/search_worldcat/example.csv`
- `inputs/search_worldcat/example.xlsx`

Note that including the `--search_my_library_holdings_first` optional argument
may increase or decrease the number of WorldCat Metadata API requests required
by the script. If you have many records to process and wish to minimize the
number of API requests made by the script, then consider running the script
*with* and *without* the `--search_my_library_holdings_first` argument on an
input file containing a *subset* of your records. The script results will tell
you how many total API requests were made, as well as how many records needed a
single WorldCat API request vs. two WorldCat API requests. Based on these
results, you can predict whether the `--search_my_library_holdings_first`
argument will result in fewer API requests for your entire dataset.

##### Description and script outputs

Searches WorldCat for each record in the input file and saves the OCLC Number.

For each row in the input file, a WorldCat search is performed using the first
available record identifier (in this order):
- `lccn_fixed` (i.e. a corrected version of the `lccn` value; this is a column
you would add to your input spreadsheet if needed in order to correct the `lccn`
value from the Alma record)
- `lccn`
- `isbn` (accepts multiple values separated by a semicolon)
- `issn` (accepts multiple values separated by a semicolon)
- `gov_doc_class_num_086` (i.e. MARC field 086: Government Document
Classification Number)
  - If `gpo_item_num_074` (i.e. MARC field 074: GPO Item Number) is also
available, then a combined search is performed (`gov_doc_class_num_086` AND
`gpo_item_num_074`).
  - If only `gpo_item_num_074` is available, then no search is performed.

Outputs the following files:
- `outputs/search_worldcat/records_with_oclc_num.csv`: Records with one WorldCat
match; hence, the OCLC Number has been found
- `outputs/search_worldcat/records_with_zero_or_multiple_worldcat_matches.csv`:
Records whose search returned zero or multiple WorldCat matches
- `outputs/search_worldcat/records_with_errors_when_searching_worldcat.csv`:
Records where an error was encountered
- If any of the above output files already exists in the directory, then it is
overwritten.

#### `update_alma_records.py`

##### Usage notes

```
usage: update_alma_records.py [-h] [-v] [--batch_size BATCH_SIZE] input_file

positional arguments:
  input_file            the name and path of the input file, which must be in either CSV (.csv) or
                        Excel (.xlsx or .xls) format (e.g. inputs/update_alma_records/filename.csv)

optional arguments:
  -h, --help            show this help message and exit
  -v, --version         show program's version number and exit
  --batch_size BATCH_SIZE
                        the number of records to batch together when making each GET request to
                        retrieve Alma records. Must be between 1 and 100, inclusive (default is 1).
                        Larger batch sizes will result in fewer total Alma API requests.

examples:
  python update_alma_records.py inputs/update_alma_records/filename.csv
  python update_alma_records.py --batch_size 10 inputs/update_alma_records/filename.csv
```

For required format of input file, see either:
- `inputs/update_alma_records/example.csv`
- `inputs/update_alma_records/example.xlsx`

For batch sizes greater than 1, note the following:
- The script will make fewer `GET` requests to the Alma API because it will
gather together multiple MMS IDs (up to the batch size) and then make a *single*
GET request for all Alma records in that particular batch. This will reduce the
total number of `GET` requests by a factor of the batch size.
- However, if any MMS ID in the batch is invalid, then the entire `GET` request
will fail and *none of the Alma records from that batch will be updated.*
- Unlike `GET` requests, the `PUT` request for *updating* an Alma record cannot
be batched. So the script will make the same number of `PUT` requests regardless
of the batch size.

##### Description and script outputs

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
- For the above output files, if an XML file with the same name already exists
in the directory, then it is overwritten. If a CSV file with the same name
already exists, then it is appended to.

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
- Recommended: Create the following directory for these XML files:
`inputs/extract_record_identifiers/xml_files_to_extract_from/`
- Create sets in Alma that contain the records whose holdings should be set in
WorldCat. Begin each set name with the same prefix (e.g. "OCLC Reclamation") to
facilitate easy retrieval of all sets.
- Export each set as an XML file:
  - You can do this by [running a job on each set](https://knowledge.exlibrisgroup.com/Alma/Product_Documentation/010Alma_Online_Help_(English)/050Administration/070Managing_Jobs/020Manual_Jobs_on_Defined_Sets).
  - For Select Job to Run, choose "Export Bibliographic Records".
  - Select the set you want to export.
  - Choose "MARC21 Bibliographic" as the Output Format and "XML" as the Physical
  Format.
  - If you want the XML file to be downloadable by others in your institution,
  choose "Institution" for Export Into Folder. Otherwise, leave it as "Private".
  - When the job is complete, download the XML file to the desired directory,
  e.g. `inputs/extract_record_identifiers/xml_files_to_extract_from/`.

For required format of the `alma_records_with_current_oclc_num` input file, see:
- `inputs/extract_record_identifiers/example_file_for_alma_records_with_current_oclc_num.csv`

##### Description and script outputs

For each XML file in the specified directory, the MMS ID and OCLC Number(s)
from each Alma record are extracted and appended to the appropriate
`outputs/extract_record_identifiers/master_list_records` CSV file:
- If an error is encountered, then the record is added to:
`outputs/extract_record_identifiers/master_list_records_with_errors.csv`
- If the record's MMS ID appears in the optional
`alma_records_with_current_oclc_num` input file, then the record is added to:
`outputs/extract_record_identifiers/master_list_records_with_current_oclc_num.csv`
- Otherwise, the record is added to:
`outputs/extract_record_identifiers/master_list_records_with_potentially_old_oclc_num.csv`
- If any of the above output files already exists in the directory, then it is
appended to (not overwritten).

---

#### `process_worldcat_records.py`

##### Usage notes

```
usage: process_worldcat_records.py [-h] [-v] [--cascade {0,1}] operation input_file

positional arguments:
  operation        the operation to be performed on each row of the input file
                   (either get_current_oclc_number, set_holding, or unset_holding)
  input_file       the name and path of the file to be processed, which must be in CSV format
                   (e.g. inputs/process_worldcat_records/set_holding/filename.csv)

optional arguments:
  -h, --help       show this help message and exit
  -v, --version    show program's version number and exit
  --cascade {0,1}  only applicable to the unset_holding operation: whether or not to execute
                   the operation if a local holdings record or local bibliographic record exists.
                   Choose either 0 or 1 (default is 0).
                   0 - don't unset holding if local holdings record or local bibliographic
                       records exists;
                   1 - unset holding and delete local holdings record and local bibliographic
                       record (if one exists)

examples:
  python process_worldcat_records.py get_current_oclc_number inputs/process_worldcat_records/get_current_oclc_number/filename.csv
  python process_worldcat_records.py set_holding inputs/process_worldcat_records/set_holding/filename.csv
  python process_worldcat_records.py --cascade 0 unset_holding inputs/process_worldcat_records/unset_holding/filename.csv
```

Required format of input file:
- For `get_current_oclc_number` operation, see:
`inputs/process_worldcat_records/get_current_oclc_number/example.csv`
- For `set_holding` operation, see:
`inputs/process_worldcat_records/set_holding/example.csv`
- For `unset_holding` operation, see:
`inputs/process_worldcat_records/unset_holding/example.csv`
- **Important note:** The `set_holding` and `unset_holding` operations can
update your institution's holdings *even when using your OCLC Sandbox WSKey.* To
avoid this during testing, only include the OCLC numbers of Test Sandbox Records
in this input file
([see above section for more details](#important-note-about-worldcat-metadata-api-sandbox-testing)).

##### Description and script outputs

Performs the specified operation on every record in the input file.

Gathers the maximum OCLC numbers possible before sending the appropriate request
to the WorldCat Metadata API.

What each `operation` does:
- `get_current_oclc_number`: For each row, check whether the given OCLC number
is the current one.
  - If so, then add the record to:
  `outputs/process_worldcat_records/get_current_oclc_number/already_has_current_oclc_number.csv`
  - If not, then add the record to:
  `outputs/process_worldcat_records/get_current_oclc_number/needs_current_oclc_number.csv`
  - If an error is encountered, then add the record to:
  `outputs/process_worldcat_records/get_current_oclc_number/records_with_errors_when_getting_current_oclc_number.csv`
- `set_holding`: For each row, set holding for the given OCLC number.
  - If holding is set successfully, then add the record to:
  `outputs/process_worldcat_records/set_holding/records_with_holding_successfully_set.csv`
  - If holding was already set, then add the record to:
  `outputs/process_worldcat_records/set_holding/records_with_holding_already_set.csv`
  - If an error is encountered, then add the record to:
  `outputs/process_worldcat_records/set_holding/records_with_errors_when_setting_holding.csv`
- `unset_holding`: For each row, unset holding for the given OCLC number.
  - If holding is unset successfully, then add the record to:
  `outputs/process_worldcat_records/unset_holding/records_with_holding_successfully_unset.csv`
  - If holding was already unset, then add the record to:
  `outputs/process_worldcat_records/unset_holding/records_with_holding_already_unset.csv`
  - If an error is encountered, then add the record to:
  `outputs/process_worldcat_records/unset_holding/records_with_errors_when_unsetting_holding.csv`
  - **Important note:** Be careful when running the `unset_holding` operation
  with `--cascade 1`. According to the
  [WorldCat Metadata API documentation](https://developer.api.oclc.org/wc-metadata)
  (search this page for the Institution Holdings section, then look for the
  `DELETE` request on the `/ih/datalist` endpoint), `cascade` with value `1`
  will unset the holding and
  *delete the local holdings record and local bibliographic record (if one exists).*
- If any of the above output files already exists in the directory, then it is
appended to (not overwritten).

---

#### `compare_alma_to_worldcat.py`

##### Usage notes

```
usage: compare_alma_to_worldcat.py [option] alma_records_file worldcat_records_directory

positional arguments:
  alma_records_file     the name and path of the CSV file containing the records
                        in Alma whose holdings **should be set in WorldCat**
                        (e.g. inputs/compare_alma_to_worldcat/alma_master_list.csv);
                        this file should consist of a single column with one
                        OCLC number per row
  worldcat_records_directory
                        the path to the directory of files containing the records
                        whose holdings **are currently set in WorldCat** for your
                        institution; each file should be in text (.txt) or
                        CSV (.csv) format and consist of a single column with one
                        OCLC number per row

example: python compare_alma_to_worldcat.py inputs/compare_alma_to_worldcat/alma_records_file.csv inputs/compare_alma_to_worldcat/worldcat_records/
```

For required format of the `alma_records_file` input file, see:
- `inputs/compare_alma_to_worldcat/example_alma_records_file.csv`

To create/populate the `worldcat_records_directory`:
- Use OCLC WorldShare to export the bibliographic records for all your
institution's WorldCat holdings.
- Use MarcEdit (which you'll need to download and install locally) to pull only
the OCLC number (035 $a) out of these records.
- This should leave you with a directory of text (.txt) files in the following
format:
```
035$a
"(OCoLC)00000001"
"(OCoLC)00000002"
"(OCoLC)00000003"
```
- Use this directory as the `worldcat_records_directory`.
- [See these instructions](https://help.oclc.org/Metadata_Services/WorldShare_Collection_Manager/Query_collections/Create_a_query_collection_to_get_a_spreadsheet_of_your_holdings/Create_a_spreadsheet_of_your_WorldCat_holdings?sl=en) for more details.

##### Description and script outputs

Compares the Alma records which *should be set in WorldCat* to the current
WorldCat holdings.

Outputs the following files:
- `outputs/compare_alma_to_worldcat/records_with_no_action_needed.csv`:
The OCLC numbers found in both the `alma_records_file` and the
`worldcat_records_directory`
- `outputs/compare_alma_to_worldcat/records_to_set_in_worldcat.csv`:
The OCLC numbers found in the `alma_records_file` but not the
`worldcat_records_directory`
- `outputs/compare_alma_to_worldcat/records_to_unset_in_worldcat.csv`:
The OCLC numbers found in the `worldcat_records_directory` but not the
`alma_records_file`
- If any of the above output files already exists in the directory, then it is
overwritten.

### Reclamation Process Overview

Here is one way you can use these scripts to perform an OCLC reclamation:

1. For all relevant Alma records without an OCLC Number, prepare input
spreadsheet(s) for `search_worldcat.py` script.
    1. Each row of the input spreadsheet must contain the record's MMS ID and at
    least one of the following record identifiers: LCCN, ISBN, ISSN, Government
    Document Classification Number (MARC field 086).
    2. For the correct column headings, see
    `inputs/search_worldcat/example.csv`.
2. Run `search_worldcat.py` script using the input spreadsheet(s) created in the
previous step.
    1. Review the 3 spreadsheets output by the script.
    2. If relevant, send the following 2 spreadsheets to your Cataloging Team
    (they'll need to manually add the OCLC Number to these Alma records):
        1. `outputs/search_worldcat/records_with_zero_or_multiple_worldcat_matches.csv`
        2. `outputs/search_worldcat/records_with_errors_when_searching_worldcat.csv`
3. Run `update_alma_records.py` script using the following input file:
`outputs/search_worldcat/records_with_oclc_num.csv` (one of the spreadsheets
output by `search_worldcat.py`).
    1. Review the 3 spreadsheets output by the script, and then *rename them*
    (that way, when you run this script again later, it will output new
    spreadsheets rather than append to these existing spreadsheets).
    2. If relevant, send `outputs/update_alma_records/records_with_errors.csv`
    to your Cataloging Team. They'll need to manually add the OCLC Number to
    these Alma records.
4. Run `extract_record_identifiers.py` script.
    1. For the `directory_with_xml_files` input, follow
    [these instructions](#extract_record_identifierspy). You'll have to finalize
    the reclamation project sets (i.e. the sets containing all the Alma records
    that should be in WorldCat) before you export them as XML files.
    2. For the `alma_records_with_current_oclc_num` input file, combine the MMS
    ID column from `outputs/update_alma_records/records_updated.csv` and
    `outputs/update_alma_records/records_with_no_update_needed.csv` (two of the
    spreadsheets output by `update_alma_records.py`). The resulting CSV file
    should have a single column named "MMS ID".
    3. Review the 3 spreadsheets output by the script.
    4. If relevant, send
    `outputs/extract_record_identifiers/master_list_records_with_errors.csv` to
    your Cataloging Team. They'll need to manually fix these Alma records (some
    possible problems might include multiple OCLC Numbers, invalid OCLC Numbers,
    or no OCLC Number at all).
5. Run `process_worldcat_records.py` script using the `get_current_oclc_number`
operation and the following input spreadsheet:
`outputs/extract_record_identifiers/master_list_records_with_potentially_old_oclc_num.csv`
(one of the spreadsheets output by `extract_record_identifiers.py`).
    1. You'll need to make sure that this input spreadsheet adheres to
    `inputs/process_worldcat_records/get_current_oclc_number/example.csv` (in
    terms of the column headings).
    2. Review the 3 spreadsheets output by the script.
    3. If relevant, send
    `outputs/process_worldcat_records/get_current_oclc_number/records_with_errors_when_getting_current_oclc_number.csv`
    to your Cataloging Team. They'll need to manually fix these Alma records.
6. Run `update_alma_records.py` script using the following input file:
`outputs/process_worldcat_records/get_current_oclc_number/needs_current_oclc_number.csv`
(one of the spreadsheets output by `process_worldcat_records.py` in the previous
step).
    1. Review the 3 spreadsheets output by the script.
    2. If relevant, send `outputs/update_alma_records/records_with_errors.csv`
    to your Cataloging Team. They'll need to manually add the OCLC Number to
    these Alma records.
7. Create the Alma Master List spreadsheet, which contains the OCLC number of
each Alma record whose holding *should be set in WorldCat* for your institution
(this CSV file should have a single column named "OCLC Number"). Populate this
spreadsheet as follows:
    1. Add all OCLC numbers from
    `outputs/extract_record_identifiers/master_list_records_with_current_oclc_num.csv`
    (one of the spreadsheets output by `extract_record_identifiers.py`).
    2. Add all OCLC numbers from
    `outputs/process_worldcat_records/get_current_oclc_number/already_has_current_oclc_number.csv`
    (one of the spreadsheets output by `process_worldcat_records.py` using the
    `get_current_oclc_number` operation).
    3. Add all OCLC numbers from the following spreadsheets (which were output
    by `update_alma_records.py` in the previous step):
        1. `outputs/update_alma_records/records_updated.csv`
        2. `outputs/update_alma_records/records_with_no_update_needed.csv`
8. Create the WorldCat Holdings List, a directory of `.txt` files containing the
OCLC number for all records whose holdings *are currently set in WorldCat* for
your institution (each file should contain a single column named "035$a").
    1. To do this, use OCLC WorldShare to export the bibliographic records for
    all your institution's holdings.
    [See these instructions](https://help.oclc.org/Metadata_Services/WorldShare_Collection_Manager/Choose_your_Collection_Manager_workflow/Query_collections/Create_a_query_collection_to_get_a_spreadsheet_of_your_holdings/Create_a_spreadsheet_of_your_WorldCat_holdings)
    for more details.
9. Run `compare_alma_to_worldcat.py` script using the Alma Master List
spreadsheet as the `alma_records_file` input and the WorldCat Holdings List
directory as the `worldcat_records_directory` input.
    1. Review the 3 spreadsheets output by the script.
10. Run `process_worldcat_records.py` script using the `set_holding` operation
and the following input spreadsheet:
`outputs/compare_alma_to_worldcat/records_to_set_in_worldcat.csv` (one of the
spreadsheets output by `compare_alma_to_worldcat.py`).
    1. Review the 3 spreadsheets output by the script.
    2. If relevant, send
    `outputs/process_worldcat_records/set_holding/records_with_errors_when_setting_holding.csv`
    to your Cataloging Team. For each record:
        1. They may need to manually set the holding in WorldCat.
        2. They may also want to find the corresponding Alma record and fix it.
11. Decide whether `outputs/compare_alma_to_worldcat/records_to_unset_in_worldcat.csv`
(one of the spreadsheets output by `compare_alma_to_worldcat.py`) represents the
records you truly want to unset.
    1. This spreadsheet represents the OCLC numbers found in the
    `worldcat_records_directory` but not the `alma_records_file`. So you have to
    be sure that the `alma_records_file` (i.e. the Alma Master List) contains
    *all* records whose holdings should be set in WorldCat for your institution.
    2. If the `alma_records_file` is missing relevant records (perhaps because
    your Cataloging Team is manually fixing these Alma records), then
    `outputs/compare_alma_to_worldcat/records_to_unset_in_worldcat.csv` will
    contain records that *should not be unset*.
    3. There are different reasons why the `alma_records_file` might be missing
    relevant records. For example, scripts may have encountered errors with
    certain records.
    4. So in addition to reviewing
    `outputs/compare_alma_to_worldcat/records_to_unset_in_worldcat.csv`, you may
    want to manually review the other scripts' outputs (especially the error
    spreadsheets).
12. If you have a `records_to_unset_in_worldcat.csv` file that you are
comfortable is accurate, then run the `process_worldcat_records.py` script using
the `unset_holding` operation with this input file.
    1. [See these instructions](#process_worldcat_recordspy) for more details.
    2. Review the 3 spreadsheets output by the script.
    3. If relevant, send
    `outputs/process_worldcat_records/unset_holding/records_with_errors_when_unsetting_holding.csv`
    to your Cataloging Team. For each record:
        1. They may need to manually unset the holding in WorldCat.
        2. They may also want to find the corresponding Alma record and fix it.

### Maintainers

@scottsalvaggio

### Acknowledgements

@freyesdulib, @jrynhart, @kimpham54

## Contact

Ways to get in touch:

- Contact the Digital Infrastructure & Technology Coordinator at
[University of Denver, Library Technology Services](https://library.du.edu/general-information/directory)
- Create an issue in this repository
