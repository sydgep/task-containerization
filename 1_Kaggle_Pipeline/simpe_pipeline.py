import os
import docker
import pandas as pd
from prefect import task, flow, get_run_logger
from prefect.artifacts import create_table_artifact

# --- Directories ---
DATASET_DIR = "/path/to/Datasets/raw"
HOST_SHARED_DIR = "/path/to/Datasets/intermediate"
os.makedirs(HOST_SHARED_DIR, exist_ok=True)

# --- Container paths ---
CONTAINER_DATASET_DIR = "/app/datasets"
CONTAINER_OUTPUT_DIR = "/app/outputs"

# --- Volume mapping ---
VOLUME_MAPPING = {
    os.path.abspath(DATASET_DIR): {"bind": CONTAINER_DATASET_DIR, "mode": "ro"},
    os.path.abspath(HOST_SHARED_DIR): {"bind": CONTAINER_OUTPUT_DIR, "mode": "rw"},
}

# Docker client
docker_client = docker.from_env()
    
# --- Tasks ---
@task
def file_extraction(input_file, output_file, columns):
    logger = get_run_logger()

    # Run container
    container_input = f"{CONTAINER_DATASET_DIR}/{input_file}"
    container_output = f"{CONTAINER_OUTPUT_DIR}/{output_file}"

    container = docker_client.containers.run(
        image="ghcr.io/sydgep/docker-images/file_extraction:latest",
        detach=True,
        volumes=VOLUME_MAPPING,
        environment={"container_input": container_input, "container_output": container_output, "columns": columns},
    )
    container.wait()
    logs = container.logs().decode()
    logger.info(f"File Extraction Logs:\n{logs}")

    # Artifact
    host_output = os.path.join(HOST_SHARED_DIR, output_file)
    if os.path.exists(host_output):
        dataset = pd.read_parquet(host_output)
        create_table_artifact(key="ingested-data", table=dataset.head(20).to_dict(orient="records"),  description=f"Portion of ingested dataset {input_file}")
        # Create table artifact for descriptive statistics           
        stats = dataset.describe().transpose().reset_index()
        stats.columns = ["column", "count", "mean", "std", "min", "25%", "50%", "75%", "max"]
        create_table_artifact(key="descriptive-statistics",  table=stats.to_dict(orient="records"),  description="Descriptive statistics")
    

@task
def label_encoder(input_file, columns, output_file):
    # Define the container parameters 
    container_input = f"{CONTAINER_OUTPUT_DIR}/{input_file}"
    container_output = f"{CONTAINER_OUTPUT_DIR}/{output_file}"

    # Run the file extraction container
    logger = get_run_logger()
    logger.info(f"Encoding of {columns} started")

    container = docker_client.containers.run(
        image="ghcr.io/sydgep/docker-images/label_encoder:latest", detach=True,  volumes=VOLUME_MAPPING,
        environment={"container_input": container_input, "columns": columns,  "container_output": container_output}
        )
    container.wait()
    logger.info(f"Label encoding of {columns} completed")
    logs = container.logs().decode()
    logger.info(f"Container logs:\n{logs}")
    
    # Check wheather the output file exists in the shared directory (for artifact creation and versioning)
    host_output = os.path.join(HOST_SHARED_DIR, output_file)
    if os.path.exists(host_output):
        try:
            dataset = pd.read_parquet(host_output)
            create_table_artifact(key="label-encoder", table=dataset.head(20).to_dict(orient="records"), description="Encode categorical columns to numeric type.")
            # Create table artifact for descriptive statistics           
            stats = dataset.describe().transpose().reset_index()
            stats.columns = ["column", "count", "mean", "std", "min", "25%", "50%", "75%", "max"]
            create_table_artifact(key="descriptive-statistics",  table=stats.to_dict(orient="records"),  description="Descriptive statistics")
        except Exception as e:
            dataset = [{"error": f"Failed to read parquet file: {str(e)}"}]
    else:
        dataset = [{"error": "File not found after feature selection"}] 
    
    
@task
def drop_nan(input_file, output_file):
    logger = get_run_logger()

    container_input = f"{CONTAINER_OUTPUT_DIR}/{input_file}"
    container_output = f"{CONTAINER_OUTPUT_DIR}/{output_file}"

    container = docker_client.containers.run(
        image="ghcr.io/sydgep/docker-images/drop_nan:latest",
        detach=True,
        volumes=VOLUME_MAPPING,
        environment={"container_input": container_input, "container_output": container_output},
    )
    container.wait()
    logs = container.logs().decode()
    logger.info(f"Drop NaN Logs:\n{logs}")

    host_output = os.path.join(HOST_SHARED_DIR, output_file)
    if os.path.exists(host_output):
        dataset = pd.read_parquet(host_output)
        create_table_artifact(key="drop-nan", table=dataset.head(20).to_dict(orient="records"), description=f"Cleaned dataset {input_file}")
        # Create table artifact for descriptive statistics           
        stats = dataset.describe().transpose().reset_index()
        stats.columns = ["column", "count", "mean", "std", "min", "25%", "50%", "75%", "max"]
        create_table_artifact(key="descriptive-statistics",  table=stats.to_dict(orient="records"),  description="Descriptive statistics")


@task
def copula_synthesizer(input_file, output_file):
    logger = get_run_logger()

    container_input = f"{CONTAINER_OUTPUT_DIR}/{input_file}"
    container_output = f"{CONTAINER_OUTPUT_DIR}/{output_file}"

    container = docker_client.containers.run(
        image="ghcr.io/sydgep/docker-images/copula_synthesizer:latest",
        detach=True,
        volumes=VOLUME_MAPPING,
        environment={"container_input": container_input, "container_output": container_output},
    )
    container.wait()
    logs = container.logs().decode()
    logger.info(f"Copula Synthesizer Logs:\n{logs}")

    host_output = os.path.join(HOST_SHARED_DIR, output_file)
    if os.path.exists(host_output):
        dataset = pd.read_parquet(host_output)
        create_table_artifact(key="synthetic-data", table=dataset.head(20).to_dict(orient="records"), description=f"Synthetic dataset from {input_file}")
        # Create table artifact for descriptive statistics           
        stats = dataset.describe().transpose().reset_index()
        stats.columns = ["column", "count", "mean", "std", "min", "25%", "50%", "75%", "max"]
        create_table_artifact(key="descriptive-statistics",  table=stats.to_dict(orient="records"),  description="Descriptive statistics")
    

@task
def statistical_evaluation(real_data_file, synthetic_data_file, output_file):
    logger = get_run_logger()

    container_input_real = f"{CONTAINER_OUTPUT_DIR}/{real_data_file}"
    container_input_synth = f"{CONTAINER_OUTPUT_DIR}/{synthetic_data_file}"
    container_output = f"{CONTAINER_OUTPUT_DIR}/{output_file}"

    container = docker_client.containers.run(
        image="ghcr.io/sydgep/docker-images/statistical_evaluation:latest",
        detach=True,
        volumes=VOLUME_MAPPING,
        environment={
            "container_input_real": container_input_real,
            "container_input_synth": container_input_synth,
            "container_output": container_output,
        },
    )
    container.wait()
    logs = container.logs().decode()
    logger.info(f"Statistical Evaluation Logs:\n{logs}")

    host_output = os.path.join(HOST_SHARED_DIR, output_file)
    if os.path.exists(host_output):
        df = pd.read_parquet(host_output)
        evaluation_results = df.to_dict(orient="records")
        create_table_artifact(key="statistical-evaluation", table=evaluation_results,  description="Statistical fidelity results")

    
@task
def utility_evaluation(real_data_file, synthetic_data_file, target_col, train_size, model_name, metrics, output_file): 
    # Define the container parameters
    container_input_real  = f"{CONTAINER_OUTPUT_DIR}/{real_data_file}"
    container_input_synth = f"{CONTAINER_OUTPUT_DIR}/{synthetic_data_file}"
    container_output      = f"{CONTAINER_OUTPUT_DIR}/{output_file}"

    # Run the container
    logger = get_run_logger()
    logger.info(f"🔍 Running utility evaluation on {synthetic_data_file} vs {real_data_file}")

    container = docker_client.containers.run(
        image="ghcr.io/sydgep/docker-images/utility_evaluation:latest", detach=True, volumes=VOLUME_MAPPING,
        environment={"container_input_real": container_input_real, "container_input_synth": container_input_synth, "container_output": container_output, "target_col": target_col, "train_size": str(train_size), "model": model_name, "metrics": metrics}
    )
    container.wait()
    logger.info("✅ Utility evaluation completed.")
    logs = container.logs().decode()
    logger.info(f"Container logs:\n{logs}")

    # Check wheather the output file exists in the host directory for artifact creation and versioning
    host_output = os.path.join(HOST_SHARED_DIR, output_file)
    if os.path.exists(host_output):
       try:
           df = pd.read_parquet(host_output)
           evaluation_results = df.to_dict(orient="records")
       except Exception as e:
          evaluation_results = [{"error": f"Failed to read evaluation file: {str(e)}"}]
    else:
       evaluation_results = [{"error": "Evaluation results file not found"}]

    create_table_artifact(key="utility-evaluation-results", table=evaluation_results, description="Utility Evaluation Results")  
    

@task
def privacy_evaluation(real_data_file, synthetic_data_file, output_file): 
    # Define the container parameters
    container_input_real  = f"{CONTAINER_OUTPUT_DIR}/{real_data_file}"
    container_input_synth = f"{CONTAINER_OUTPUT_DIR}/{synthetic_data_file}"
    container_output      = f"{CONTAINER_OUTPUT_DIR}/{output_file}"

    # Run the container
    logger = get_run_logger()
    logger.info(f"🔍 Running privacy evaluation on {synthetic_data_file} vs {real_data_file}")

    container = docker_client.containers.run(
        image="ghcr.io/sydgep/docker-images/privacy_evaluation:latest", detach=True, volumes=VOLUME_MAPPING,
        environment={"container_input_real": container_input_real, "container_input_synth": container_input_synth, "container_output": container_output}
    )
    container.wait()
    logger.info("✅ Privacy evaluation completed.")
    logs = container.logs().decode()
    logger.info(f"Container logs:\n{logs}")

    # Check wheather the output file exists in the host directory for artifact creation and versioning
    host_output = os.path.join(HOST_SHARED_DIR, output_file)
    if os.path.exists(host_output):
       try:
           df = pd.read_parquet(host_output)
           evaluation_results = df.to_dict(orient="records")
       except Exception as e:
          evaluation_results = [{"error": f"Failed to read evaluation file: {str(e)}"}]
    else:
       evaluation_results = [{"error": "Evaluation results file not found"}]

    create_table_artifact(key="privacy-evaluation-results", table=evaluation_results, description="Privacy Evaluation Results") 
    

# --- Flow ---
@flow(name="Simple Pipeline")
def sydgep_pipeline():

    file_extraction(input_file = "dataset.json", output_file = "dataset.parquet", columns = "") 
    label_encoder(input_file = "dataset.parquet", output_file = "dataset.parquet", columns = "GENDER,LUNG_CANCER")
    drop_nan(input_file = "dataset.parquet", output_file = "cleaned_data.parquet")
    copula_synthesizer(input_file = "cleaned_data.parquet", output_file = "synthetic_data.parquet")
    statistical_evaluation(real_data_file="cleaned_data.parquet", synthetic_data_file="synthetic_data.parquet", output_file="statistical_evaluation.parquet")
    privacy_evaluation(real_data_file="cleaned_data.parquet", synthetic_data_file="synthetic_data.parquet", output_file="privacy_evaluation.parquet")
    utility_evaluation(real_data_file="cleaned_data.parquet", synthetic_data_file="synthetic_data.parquet", target_col="LUNG_CANCER", train_size=0.7, model_name="SVC", metrics="accuracy,roc_auc,f1",output_file="utility_evaluation.parquet")

if __name__ == "__main__":
    sydgep_pipeline()
