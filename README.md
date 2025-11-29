# CMS Hospital Dataset Downloader

A Python script that automatically discovers, downloads, and processes all hospital-related datasets from the CMS Provider Data API.
The script is designed to run daily, perform incremental downloads, convert all CSV headers to snake_case, and save the processed files locally in parallel.


## Features

### 1. Automatically discovers hospital datasets

Fetches dataset metadata from:

https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items

Filters datasets whose metadata contains the theme “Hospitals” (checking theme, tags, title, description).

---

### 2. Downloads all CSV distributions

For each matching dataset, the script extracts all linked .csv files and downloads them.

---

### 3. Parallel, asynchronous downloading

Uses aiohttp, asyncio, and a concurrency semaphore to download many files at once for high performance.

---

### 4. Converts all CSV headers to snake_case

#### Example conversion:

Original Header	Converted

Patients Rating of the Facility ==>	patients_rating_of_the_facility

#### The transformation:
	•	lowercase
	
	•	spaces → _
	
	•	remove punctuation
	
	•	split camelCase → snake_case
	
	•	collapse repeated underscores

---

### 5. Incremental daily updates

The script stores metadata in:

metadata.json

#### This includes:
	•	URL
	
	•	filename
	
	•	last_modified header
	
	•	ETag
	
	•	row, column counts
	
	•	download timestamp

#### Next day:

The script checks remote ETag / Last-Modified and skips files that haven’t changed.

This makes the script safe and efficient for daily scheduled runs.

---

### 6. Platform independent

#### Runs on:
	•	macOS
	
	•	Linux
	
	•	Windows

Uses only pip-installable libraries.

---

## Project Structure

```
project_root/
│
├── cms_hospitals_downloader.py     # Main script
├── requirements.txt                # Dependencies
├── metadata.json                   # Auto-generated metadata tracking
│
└── data/                           # Auto-generated folder of processed CSVs
     ├── <dataset_id>/              # One folder per dataset
     │      ├── <file>.csv
     ├── <dataset_id>/              
     │      ├── <file>.csv

```

All downloaded and processed CSVs are inside /data/<dataset_id>/.


## Installation

#### 1. Clone or download this project

```
git clone <your-repo-url>

cd <project-folder>

```

#### 2. Create a virtual environment

```

python3 -m venv ondemangrp_venv

source ondemangrp_venv/bin/activate     # macOS/Linux

ondemangrp_venv\Scripts\activate        # Windows

```

#### 3. Install dependencies

pip3 install -r requirements.txt


---

## How to Run the Script

### Run directly:

python cms_hospitals_downloader.py

After completion, you will see:
	•	metadata.json updated
	
	•	Processed CSVs in the data/ folder
	
	•	Headers fully converted to snake_case

---

## Running Daily (Scheduled Execution)

Script is designed to be run daily using a scheduler:

### macOS / Linux (cron)

0 2 * * * /usr/bin/python3 /path/to/cms_hospitals_downloader.py

### Windows (Task Scheduler)
	•	Trigger: Daily → 2:00 AM
	
	•	Action: python cms_hospitals_downloader.py

The script is idempotent, meaning it only downloads updated files.

---

## How Metadata Tracking Works (metadata.json)

### Example entry:

```
{
  "HOSPITAL_GENERAL_INFORMATION::https://data.cms.gov/.../file.csv": {
    "url": "...",
    "filename": "Hospital_General_Information.csv",
    "etag": "\"abcd123\"",
    "last_modified": "Mon, 13 Jan 2025 10:18:01 GMT",
    "downloaded_at": "2025-11-29T16:44:00Z",
    "rows": 5431,
    "cols": 41
  }
}
```
#### On each run, the script:
	1.	Performs a HEAD request
	
	2.	Compares ETag / Last-Modified with saved values
	
	3.	Skips downloading if unchanged

This ensures efficient daily operation.

---

## Snake Case Conversion

#### The script uses this logic:
	•	Remove punctuation
	
	•	Insert underscores between words
	
	•	Split camelCase
	
	•	Convert to lowercase
	
	•	Collapse multiple underscores

### Example:

#### Input:

"Patients' Rating of the Facility Linear Mean Score"

#### Output:

patients_rating_of_the_facility_linear_mean_score

All downloaded CSV headers have been automatically processed by pandas and overwritten in snake_case.

---

## Sample Output Verification

### Use:

head -n 1 data/**/*.csv

### Example output:

facility_id,facility_name,address,city_town,state,zip_code,...

This confirms snake_case conversion.
