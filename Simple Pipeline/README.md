# Simple Pipeline — Setup & Execution Guide

This guide explains how to run the **Prefect-based pipeline** using the provided dataset and Docker-based task containers on **Linux, Windows, and macOS**.

---
## Step0: Install Docker Engine / Docker Desktop
Before running the pipeline, ensure Docker is installed and running:

- 🐧 **Linux**: Install Docker Engine  
- 🪟 **Windows**: Install Docker Desktop (requires WSL2)  
- 🍎 **macOS**: Install Docker Desktop  
[Ref](https://www.docker.com/get-started/)
Verify installation:
```bash
docker run hello-world

## 📁 Step 1 — Clone or Copy the Project
### 🔧 Modify Paths in the Pipeline

Open `simple_pipeline.py` in your preferred IDE or text editor.

Update the following variables:

```python
DATASET_DIR = "/path/to/Datasets/raw"
HOST_SHARED_DIR = "/path/to/Datasets/intermediate"

## Step 2 — Create Virtual Environment & Install Dependencies
Required Libraries
prefect==3.6.20
docker
pandas
pyarrow
fastparquet
🐧 Linux / 🍎 macOS
```
python3 -m venv sydgep_venv
source sydgep_venv/bin/activate
pip install prefect==3.6.20 docker pandas pyarrow fastparquet
```

```
🪟 Windows (PowerShell)
py -m venv sydgep_venv 
.\sydgep_venv\Scripts\Activate.ps1

pip install prefect==3.6.20 docker pandas pyarrow fastparquet
```

## Step 3 — Configure & Start Prefect Server
### Start Prefect Server
```prefect server start
```
### Configure API URL (new terminal)
```prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api```

## Step 4: Execute the pipeline
```python simple_pipeline.py```
