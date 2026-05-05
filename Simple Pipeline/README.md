# Simple Pipeline — Setup & Execution Guide

This guide explains how to run the **Prefect-based pipeline** using the provided dataset and Docker-based task containers on **Linux, Windows, and macOS**.

---
## Step 0: Install Docker Engine / Docker Desktop
Before running the pipeline, ensure Docker is installed and running. Go to [link](https://www.docker.com/get-started/).

## Step 1 — Clone or Copy the Project
Open `simple_pipeline.py` in your preferred IDE/text editor and update the paths of the `DATASET_DIR` & `HOST_SHARED_DIR`. E.g

```python
DATASET_DIR = "/homelocal/user/Datasets" # to access the real datasets
HOST_SHARED_DIR = "/homelocal/user/Datasets/intermediate"  # to store the intermediate files
```

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
prefect server start
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
```

## Step 4: Execute the pipeline
```python simple_pipeline.py```

Access the prefect UI on your browser via: http://127.0.0.1:4200
