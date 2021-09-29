# OCLC Reclamation

## Table of Contents

- [README](#readme)
- [Contact](#contact)

## README

### Background

Objective: To create Python scripts to reconcile a library's local holdings in
Ex Libris Alma with OCLC's WorldCat database.

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
- Recommended: Create and activate virtual environment (for example, see
  [venv](https://docs.python.org/3/library/venv.html))
- Run `pip install -r requirements.txt`
- Add `.env` file to root folder (you can copy `.env-example`) and initialize
  the variables:
  - `ALMA_BIBS_API_URL`
    - If you're in North America, use
    `https://api-na.hosted.exlibrisgroup.com/almaws/v1/bibs/`
    - If not,
    [look here](https://developers.exlibrisgroup.com/alma/apis/#calling) for the
    base URL for your geographic region
  - `ALMA_API_KEY`
    - [Instructions for creating an API key](https://developers.exlibrisgroup.com/alma/apis/#using)
- Create `alma-test.xlsx` spreadsheet inside `xlsx` folder (you can copy
  `alma-test-example.xlsx`)
  - `alma-test.xlsx` should contain:
    - `MMS ID` and `OCLC Number` column headings
    - at least one row with a valid `MMS ID` and `OCLC Number`
- Run `python alma-api-test.py`

### Maintainers

@scottsalvaggio

### Acknowledgements

@freyesdulib, @jrynhart, @kimpham54

## Contact

Ways to get in touch:

* Contact the Digital Infrastructure & Technology Coordinator at [University of Denver, Library Technology Services](https://library.du.edu/contact/department-directory.html)
* Create an issue in this repository
