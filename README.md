# oracle-ebs-extractor

Extracts an object-centric event log from an instnace of Oracle EBS supported by the Oracle SQL database.

## Installation

- *pip install -U -r requirements.txt*
- *pip install --no-deps pm4pymdl*
- *pip install --no-deps sapextractor*

Also, the Oracle Client libraries need to be installed, as described in:
[this website](https://cx-oracle.readthedocs.io/en/latest/user_guide/installation.html).

## Usage

- Replace the credentials inside *oracle_ebs_table_script.py* in order to allow for the connection to the Oracle database.
- Launch the script *oracle_ebs_table_script.py* (without arguments) in order to download the database tables and store them as CSV.
- Finally, launch the script *oracle_ebs_ocel.py* (without arguments) in order to create the OCEL 2.0 event log. You can modify
at the bottom of the script the target format (or use one of the OCEL 1.0 formats).
