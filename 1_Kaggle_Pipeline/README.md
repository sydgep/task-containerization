# Simple Pipeline 

This pipeline ingest real tabular datasets (from Kaggle), preprocess, clean, generate and evaluate synthetic data for **lung cancer prediction**. The goal is to orchestrate the pipeline on **Linux, Windows, and macOS**. 

---
## Pre-requisite — Install Docker Engine / Docker Desktop
For Docker installation procedures on various operating systems, go to [link](https://www.docker.com/get-started/).

## Step 1 — Copy or download the `simple_pipeline.py` and the datasets files. 
Open `simple_pipeline.py` in your preferred IDE/text editor and update the paths of the `DATASET_DIR` & `HOST_SHARED_DIR`. E.g

```python
DATASET_DIR = "/homelocal/user/Datasets/raw" # to access the real datasets
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
prefect server start (terminal 1)
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api  (terminal 2)
```

## Step 4 — Execute the pipeline
```python simple_pipeline.py```

Access the prefect UI on your browser via: http://127.0.0.1:4200
