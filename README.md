# ReArc Data Engineering Quest

This repository contains my submission for the ReArc Data Engineering Quest. It includes the source code for data ingestion scripts, a data analysis notebook, and the infrastructure-as-code for an automated data pipeline.

---

## Project Overview

The goal of this quest was to build a complete data pipeline that sources two datasets, performs analysis, and automates the workflow using AWS serverless technologies.

The final architecture consists of:
* **S3 Bucket** as a central data lake.
* **Two Python scripts** for ingesting data from a website (BLS.gov) and a public API.
* **A Jupyter Notebook** for data cleaning and analysis using Pandas.
* **An event-driven pipeline** using S3, SQS, and Lambda, defined with AWS SAM.

---

## How to Run This Project

### Prerequisites
* Python 3.9+
* An AWS account
* AWS CLI configured
* AWS SAM CLI installed
* Docker

### 1. Data Ingestion (Parts 1 & 2)
The ingestion scripts are located in the `part_1_and_2_scripts/` directory.

1.  Create an S3 bucket.
2.  Update the `BUCKET_NAME` variable in both scripts with your bucket name.
3.  Run the scripts:
    ```bash
    python3 sync_bls_data.py
    python3 fetch_population_data.py
    ```

### 2. Data Analysis (Part 3)
The analysis notebook is located in `part_3_analytics/data_analysis.ipynb`. It can be run using Jupyter Notebook and will load the data from the S3 bucket to produce the analytical results.

### 3. Automated Pipeline (Part 4)
The serverless pipeline is defined in the `part_4_infrastructure/rearc-pipeline/` directory.

1.  Navigate to the directory: `cd part_4_infrastructure/rearc-pipeline`
2.  Build the SAM application: `sam build --use-container`
3.  Deploy the pipeline: `sam deploy --guided`
    * You will be prompted to enter the S3 bucket name during deployment.

---

## Notes and Challenges

* **Part 1:** Successfully handled the `403 Forbidden` error from the BLS website by setting a valid `User-Agent` header. Implemented logic to prevent re-uploading unchanged files.

---

## Submission Links

* **Public S3 Bucket Link:** `s3://mayank-rearc-quest-2025/`
* **This GitHub Repository:** https://github.com/mayanksolanki9/mayank-rearc-quest-2025.git