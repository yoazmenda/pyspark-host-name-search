# Intro
https://ducks.party/ct

This websites lists scanned and crawled domain names as part of the https://scanner.ducks.party/ project.

it publishes dump files containing all the scanned hostnames.

This project aims to help woth searching withing those files - with about 300 million records using PySpark

# Usage
```bash

# clone the repo
git clone https://github.com/yoazmenda/pyspark-host-name-search
cd pyspark-host-name-search

# create a virtual env
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

python app.py
```

(edit your desired string within the app.py file)
