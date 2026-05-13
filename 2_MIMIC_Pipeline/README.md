# MIMIC-IV Pipeline 

This pipeline ingest several datasets form MIMIC_IV, preprocess, integrate, generate and evaluate synthetic dataset for mortality prediction. The objective is to orchestrate the pipeline on **Linux, Windows, and macOS**. 

---
## Pre-requisites
### A - Install Docker Engine / Docker Desktop
For Docker installation procedures on various operating systems, go to [link](https://www.docker.com/get-started/).
### B - Download MIMIC-IV Datasets
This is a restricted-access resource. To access the datasets, you must be **be a credentialed user**,  **complete required training** and **sign the data use agreement for the project**
[MIMIC-IV](https://physionet.org/content/mimiciv/2.0/)

## Step 1 — Copy or download the `mimic_pipeline.py` file. 
Open `mimic_pipeline.py` in your preferred IDE/text editor and update the paths of the `DATASET_DIR` & `HOST_SHARED_DIR`.

## Step 2 — Create Virtual Environment & Install Dependencies
### Linux & macOS users
```
python3 -m venv sydgep_venv
source sydgep_venv/bin/activate
pip install prefect==3.6.20 docker pandas pyarrow fastparquet
```
### Windows users (PowerShell)
```
py -m venv sydgep_venv 
.\sydgep_venv\Scripts\Activate.ps1
pip install prefect==3.6.20 docker pandas pyarrow fastparquet
```

## Step 3 — Start Prefect Server

```
prefect server start (terminal 1)
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api  (terminal 2)
```

## Step 4 — Execute the pipeline
```python mimic_pipeline.py```

Access the prefect UI on your browser via: http://127.0.0.1:4200
