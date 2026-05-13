import os
import docker
import pandas as pd
from prefect import task, flow, get_run_logger
from prefect.artifacts import create_table_artifact


# --- Directories ---
DATASET_DIR = "/homelocal/ms271520/INSAFEDARE/Datasets/mimiciv/2.0/hosp"
HOST_SHARED_DIR = "/homelocal/ms271520/INSAFEDARE/Datasets/mimiciv/outputs"
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
@task(name="Ingestion")   
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
        create_table_artifact(key="ingested-data", table=dataset.head(20).to_dict(orient="records"),  description=f"Portion of {input_file}")
        # Create table artifact for descriptive statistics           
        stats = dataset.describe().transpose().reset_index()
        stats.columns = ["column", "count", "mean", "std", "min", "25%", "50%", "75%", "max"]
        create_table_artifact(key="descriptive-statistics",  table=stats.to_dict(orient="records"),  description="Descriptive statistics")

@task(name="Measurement Counting")
def count(input_file, groupby, output_colname, output_file):
    # Define the container parameters 
    container_input = f"{CONTAINER_OUTPUT_DIR}/{input_file}"
    container_output = f"{CONTAINER_OUTPUT_DIR}/{output_file}"

    # Run the file extraction container
    logger = get_run_logger()
    logger.info(f"Measurement counting from {input_file} started")

    container = docker_client.containers.run(
        image="ghcr.io/sydgep/docker-images/count:latest", detach=True,  volumes=VOLUME_MAPPING,
        environment={"container_input": container_input, "groupby": groupby, "output_colname": output_colname, "container_output": container_output}
        )
    container.wait()
    logger.info(f"Measurement counting from {input_file} completed")
    logs = container.logs().decode()
    logger.info(f"Container logs:\n{logs}")
    
    # Check wheather the output file exists in the shared directory (for artifact creation and versioning)
    host_output = os.path.join(HOST_SHARED_DIR, output_file)
    if os.path.exists(host_output):
        try:
            dataset = pd.read_parquet(host_output)
            create_table_artifact(key="counted-measurement", table=dataset.head(20).to_dict(orient="records"), description=f"{output_colname} counted from {input_file}.")
            # Create table artifact for descriptive statistics           
            stats = dataset.describe().transpose().reset_index()
            stats.columns = ["column", "count", "mean", "std", "min", "25%", "50%", "75%", "max"]
            create_table_artifact(key="descriptive-statistics",  table=stats.to_dict(orient="records"),  description="Descriptive statistics")
        except Exception as e:
            dataset = [{"error": f"Failed to read parquet file: {str(e)}"}]
    else:
        dataset = [{"error": "File not found after feature extraction"}]

@task(name="Measurement Extraction")
def extract_measurement(input_file, name_col, value_col, measurement_name, match_type, output_file):
    # Define the container parameters 
    container_input = f"{CONTAINER_OUTPUT_DIR}/{input_file}"
    container_output = f"{CONTAINER_OUTPUT_DIR}/{output_file}"

    # Run the file extraction container
    logger = get_run_logger()
    logger.info(f"Measurement extraction from {input_file} started")

    container = docker_client.containers.run(
        image="ghcr.io/sydgep/docker-images/extract_measurement:latest", detach=True,  volumes=VOLUME_MAPPING,
        environment={"container_input": container_input, "name_col": name_col, "value_col": value_col, "measurement_name": measurement_name, "match_type": match_type, "container_output": container_output}
        )
    container.wait()
    logger.info(f"Measurement extraction from {input_file} completed")
    logs = container.logs().decode()
    logger.info(f"Container logs:\n{logs}")
    
    # Check wheather the output file exists in the shared directory (for artifact creation and versioning)
    host_output = os.path.join(HOST_SHARED_DIR, output_file)
    if os.path.exists(host_output):
        try:
            dataset = pd.read_parquet(host_output)
            create_table_artifact(key="extracted-measurement", table=dataset.head(20).to_dict(orient="records"), description=f"{measurement_name} extracted from {input_file}.")
            # Create table artifact for descriptive statistics           
            stats = dataset.describe().transpose().reset_index()
            stats.columns = ["column", "count", "mean", "std", "min", "25%", "50%", "75%", "max"]
            create_table_artifact(key="descriptive-statistics",  table=stats.to_dict(orient="records"),  description="Descriptive statistics")
        except Exception as e:
            dataset = [{"error": f"Failed to read parquet file: {str(e)}"}]
    else:
        dataset = [{"error": "File not found after feature extraction"}]


@task(name="Feature Expansion")
def expand_feature(input_file, feature, expand_on, output_file):
    # Define the container parameters 
    container_input = f"{CONTAINER_OUTPUT_DIR}/{input_file}"
    container_output = f"{CONTAINER_OUTPUT_DIR}/{output_file}"

    # Run the file extraction container
    logger = get_run_logger()
    logger.info(f"Feature expansion from {input_file} started")

    container = docker_client.containers.run(
        image="ghcr.io/sydgep/docker-images/expand_feature:latest", detach=True,  volumes=VOLUME_MAPPING,
        environment={"container_input": container_input, "feature": feature, "expand_on": expand_on, "container_output": container_output}
        )
    container.wait()
    logger.info(f"Feature expansion from {input_file} completed")
    logs = container.logs().decode()
    logger.info(f"Container logs:\n{logs}")
    
    # Check wheather the output file exists in the shared directory (for artifact creation and versioning)
    host_output = os.path.join(HOST_SHARED_DIR, output_file)
    if os.path.exists(host_output):
        try:
            dataset = pd.read_parquet(host_output)
            create_table_artifact(key="expand-feature", table=dataset.head(20).to_dict(orient="records"), description=f"Expanded feature from {input_file}")
            # Create table artifact for descriptive statistics           
            stats = dataset.describe().transpose().reset_index()
            stats.columns = ["column", "count", "mean", "std", "min", "25%", "50%", "75%", "max"]
            create_table_artifact(key="descriptive-statistics",  table=stats.to_dict(orient="records"),  description="Descriptive statistics")
        except Exception as e:
            dataset = [{"error": f"Failed to read parquet file: {str(e)}"}]
    else:
        dataset = [{"error": "File not found after feature expansion"}]        
    

@task(name="Cohort Selection")
def select_cohort(input_file, target_col, id_col, match_on, match_on_len, output_file):
    # Define the container parameters 
    container_input = f"{CONTAINER_OUTPUT_DIR}/{input_file}"
    container_output = f"{CONTAINER_OUTPUT_DIR}/{output_file}"

    # Run the file extraction container
    logger = get_run_logger()
    logger.info(f"Cohort selection from {input_file} started")

    container = docker_client.containers.run(
        image="ghcr.io/sydgep/docker-images/select_cohort:latest", detach=True,  volumes=VOLUME_MAPPING,
        environment={"container_input": container_input, "container_output": container_output, "target_col": target_col, "id_col": id_col, "match_on": match_on, "match_on_len": match_on_len}
        )
    container.wait()
    logger.info(f"Cohort selection from {input_file} completed")
    logs = container.logs().decode()    
    logger.info(f"Container logs:\n{logs}")
    
    # Check wheather the output file exists in the shared directory (for artifact creation and versioning)
    host_output = os.path.join(HOST_SHARED_DIR, output_file)
    if os.path.exists(host_output):
        try:
            dataset = pd.read_parquet(host_output)
            create_table_artifact(key="selected-cohort", table=dataset.head(10).to_dict(orient="records"), description=f"Cohort selected from the {input_file}.")
            # Create table artifact for descriptive statistics           
            stats = dataset.describe().transpose().reset_index()
            stats.columns = ["column", "count", "mean", "std", "min", "25%", "50%", "75%", "max"]
            create_table_artifact(key="descriptive-statistics",  table=stats.to_dict(orient="records"),  description="Descriptive statistics")
        except Exception as e:
            dataset = [{"error": f"Failed to read parquet file: {str(e)}"}]
    else:
        dataset = [{"error": "File not found after cohort selection"}]


@task(name="Merge Data")
def merge_data(input_file_base, input_file_merge, merge_on, how, output_file):
    # Define the container parameters 
    container_base_input = f"{CONTAINER_OUTPUT_DIR}/{input_file_base}"
    container_merge_input = f"{CONTAINER_OUTPUT_DIR}/{input_file_merge}"
    container_output = f"{CONTAINER_OUTPUT_DIR}/{output_file}"

    # Run the file extraction container
    logger = get_run_logger()
    logger.info(f"Data merging between {input_file_base} and {input_file_merge} started")

    container = docker_client.containers.run(
        image="ghcr.io/sydgep/docker-images/merge_data:latest", detach=True,  volumes=VOLUME_MAPPING,
        environment={"container_base_input": container_base_input, "container_merge_input": container_merge_input, "container_output": container_output, "merge_on": merge_on, "how": how}
        )
    container.wait()
    logger.info(f"Data merging between {input_file_base} and {input_file_merge} completed")
    logs = container.logs().decode()
    logger.info(f"Container logs:\n{logs}")
    
    # Check wheather the output file exists in the shared directory (for artifact creation and versioning)
    host_output = os.path.join(HOST_SHARED_DIR, output_file)
    if os.path.exists(host_output):
        try:
            dataset = pd.read_parquet(host_output)
            create_table_artifact(key="merged-dataset", table=dataset.head(20).to_dict(orient="records"), description=f"{input_file_base} and {input_file_merge} are merged based on {merge_on}")
            # Create table artifact for descriptive statistics           
            stats = dataset.describe().transpose().reset_index()
            stats.columns = ["column", "count", "mean", "std", "min", "25%", "50%", "75%", "max"]
            create_table_artifact(key="descriptive-statistics",  table=stats.to_dict(orient="records"),  description="Descriptive statistics")
        except Exception as e:
            dataset = [{"error": f"Failed to read parquet file: {str(e)}"}]
    else:
        dataset = [{"error": "File not found after merging"}]        


@task(name="Merge on Date")
def merge_on_date(input_file_base, date_col_base, input_file_merge, date_col_merge, merge_on, direction, output_file):
    # Define container paths
    container_base_input = f"{CONTAINER_OUTPUT_DIR}/{input_file_base}"
    container_merge_input = f"{CONTAINER_OUTPUT_DIR}/{input_file_merge}"
    container_output = f"{CONTAINER_OUTPUT_DIR}/{output_file}"

    logger = get_run_logger()
    logger.info(f"Temporal merging between {input_file_base} and {input_file_merge} started")

    # Environment variables for the container
    env_vars = {
        "container_base_input": container_base_input,
        "container_merge_input": container_merge_input,
        "container_output": container_output,
        "date_col_base": date_col_base,
        "date_col_merge": date_col_merge,
        "merge_on": merge_on,
        "direction": direction,
    }

    # Run the container
    container = docker_client.containers.run(
        image="ghcr.io/sydgep/docker-images/merge_on_date:latest",
        detach=True,
        volumes=VOLUME_MAPPING,
        environment=env_vars
    )
    container.wait()
    logs = container.logs().decode()
    logger.info(f"Temporal merging between {input_file_base} and {input_file_merge} completed")
    logger.info(f"Container logs:\n{logs}")

    # Check the output
    host_output = os.path.join(HOST_SHARED_DIR, output_file)
    if os.path.exists(host_output):
        try:
            dataset = pd.read_parquet(host_output)
            # Convert all datetime columns to ISO strings
            for col in dataset.select_dtypes(include=["datetime64[ns]"]).columns:
                dataset[col] = dataset[col].astype(str)

            create_table_artifact(key="merged-on-date", table=dataset.head(20).to_dict(orient="records"),   description=f"{input_file_base} merged with {input_file_merge}")
            # Create table artifact for descriptive statistics           
            stats = dataset.describe().transpose().reset_index()
            stats.columns = ["column", "count", "mean", "std", "min", "25%", "50%", "75%", "max"]
            create_table_artifact(key="descriptive-statistics",  table=stats.to_dict(orient="records"),  description="Descriptive statistics")
        except Exception as e:
            logger.warning(f"Artifact creation failed: {e}")
    else:
        logger.error("File not found after temporal merging")
    
    
@task(name="Feature Selection")
def feature_selection(input_file, columns, output_file):
    # Define the container parameters 
    container_input = f"{CONTAINER_OUTPUT_DIR}/{input_file}"
    container_output = f"{CONTAINER_OUTPUT_DIR}/{output_file}"

    # Run the file extraction container
    logger = get_run_logger()
    logger.info(f"Feature selection  from {input_file} started")

    container = docker_client.containers.run(
        image="ghcr.io/sydgep/docker-images/feature_selection:latest", detach=True,  volumes=VOLUME_MAPPING,
        environment={"container_input": container_input, "columns": columns,  "container_output": container_output}
        )
    container.wait()
    logger.info(f"Feature selection from {input_file} completed")
    logs = container.logs().decode()
    logger.info(f"Container logs:\n{logs}")
    
    # Check wheather the output file exists in the shared directory (for artifact creation and versioning)
    host_output = os.path.join(HOST_SHARED_DIR, output_file)
    if os.path.exists(host_output):
        try:
            dataset = pd.read_parquet(host_output)
            create_table_artifact(key="feature-selection", table=dataset.head(20).to_dict(orient="records"), description=f"Features selected from {input_file}")
            # Create table artifact for descriptive statistics           
            stats = dataset.describe().transpose().reset_index()
            stats.columns = ["column", "count", "mean", "std", "min", "25%", "50%", "75%", "max"]
            create_table_artifact(key="descriptive-statistics",  table=stats.to_dict(orient="records"),  description="Descriptive statistics")
        except Exception as e:
            dataset = [{"error": f"Failed to read parquet file: {str(e)}"}]
    else:
        dataset = [{"error": "File not found after feature selection"}]


@task(name="Encoding")
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


@task(name="Cleaning")
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


@task(name="Generation")
def ctgan_synthesizer(input_file, output_file):
    # Define the container parameters 
    container_input = f"{CONTAINER_OUTPUT_DIR}/{input_file}"
    container_output = f"{CONTAINER_OUTPUT_DIR}/{output_file}"

    # Run the container
    logger = get_run_logger()
    logger.info(f"Synthetic data generation from {input_file} started")

    container = docker_client.containers.run(
        image="ghcr.io/sydgep/docker-images/ctgan_synthesizer:latest", detach=True,  volumes=VOLUME_MAPPING,
        environment={"container_input": container_input,  "container_output": container_output}
        )
    container.wait()
    logger.info(f"Synthetic data generation from {input_file} completed")
    logs = container.logs().decode()
    logger.info(f"Container logs:\n{logs}")
    
    # Check wheather the output file exists in the shared directory (for artifact creation and versioning)
    host_output = os.path.join(HOST_SHARED_DIR, output_file)
    if os.path.exists(host_output):
        try:
            dataset = pd.read_parquet(host_output)
            create_table_artifact(key="synthetic-data-ctgan", table=dataset.head(20).to_dict(orient="records"), description=f"Synthetic data generated from {input_file}")
            # Create table artifact for descriptive statistics           
            stats = dataset.describe().transpose().reset_index()
            stats.columns = ["column", "count", "mean", "std", "min", "25%", "50%", "75%", "max"]
            create_table_artifact(key="descriptive-statistics",  table=stats.to_dict(orient="records"),  description="Descriptive statistics")
        except Exception as e:
            dataset = [{"error": f"Failed to read parquet file: {str(e)}"}]
    else:
        dataset = [{"error": "File not found after synthetic data generation"}]
        
     

@task(name="Generation")
def vae_synthesizer(input_file, output_file):
    # Define the container parameters 
    container_input = f"{CONTAINER_OUTPUT_DIR}/{input_file}"
    container_output = f"{CONTAINER_OUTPUT_DIR}/{output_file}"

    # Run the container
    logger = get_run_logger()
    logger.info(f"Synthetic data generation from {input_file} started")

    container = docker_client.containers.run(
        image="ghcr.io/sydgep/docker-images/vae_synthesizer:latest", detach=True,  volumes=VOLUME_MAPPING,
        environment={"container_input": container_input,  "container_output": container_output}
        )
    container.wait()
    logger.info(f"Synthetic data generation from {input_file} completed")
    logs = container.logs().decode()
    logger.info(f"Container logs:\n{logs}")
    
    # Check wheather the output file exists in the shared directory (for artifact creation and versioning)
    host_output = os.path.join(HOST_SHARED_DIR, output_file)
    if os.path.exists(host_output):
        try:
            dataset = pd.read_parquet(host_output)
            create_table_artifact(key="synthetic-data-vae", table=dataset.head(20).to_dict(orient="records"), description=f"Synthetic data generated from {input_file}")
            # Create table artifact for descriptive statistics           
            stats = dataset.describe().transpose().reset_index()
            stats.columns = ["column", "count", "mean", "std", "min", "25%", "50%", "75%", "max"]
            create_table_artifact(key="descriptive-statistics",  table=stats.to_dict(orient="records"),  description="Descriptive statistics")
        except Exception as e:
            dataset = [{"error": f"Failed to read parquet file: {str(e)}"}]
    else:
        dataset = [{"error": "File not found after synthetic data generation"}]
        

@task(name="Generation")
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
        create_table_artifact(key="synthetic-data-copula", table=dataset.head(20).to_dict(orient="records"), description=f"Synthetic dataset from {input_file}")
        # Create table artifact for descriptive statistics           
        stats = dataset.describe().transpose().reset_index()
        stats.columns = ["column", "count", "mean", "std", "min", "25%", "50%", "75%", "max"]
        create_table_artifact(key="descriptive-statistics",  table=stats.to_dict(orient="records"),  description="Descriptive statistics")
    

@task(name="Evaluation")
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


@task(name="Evaluation")
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
    

@task(name="Evaluation")
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
@flow(name="MIMIC Pipeline - Mortality Prediction")
def sydgep_pipeline():

    file_extraction(input_file = "diagnoses_icd.csv.gz", output_file = "diagnoses.parquet", columns = "hadm_id, icd_code, seq_num") 
    select_cohort(input_file="diagnoses.parquet", output_file="cohort.parquet", target_col="icd_code", id_col="hadm_id", match_on="I50,428", match_on_len="3")
    count(input_file = "diagnoses.parquet", groupby="hadm_id", output_colname="n_diagnoses", output_file = "diagnoses.parquet")             
    
    file_extraction(input_file = "prescriptions.csv.gz", output_file = "prescriptions.parquet", columns = "hadm_id, subject_id")   
    count(input_file = "prescriptions.parquet", groupby="hadm_id", output_colname="n_medications", output_file = "prescriptions.parquet")       
    
    file_extraction(input_file = "admissions.csv.gz", output_file = "admissions.parquet", columns = "hadm_id, subject_id, admittime, dischtime, admission_type, admission_location, hospital_expire_flag")  
    
    file_extraction(input_file = "patients.csv.gz", output_file = "patients.parquet",   columns = "subject_id, anchor_age, gender")     
    
    file_extraction(input_file = "omr.csv.gz", output_file = "omr.parquet", columns = "subject_id, chartdate, result_name, result_value")    
    extract_measurement(input_file="omr.parquet", name_col="result_name", value_col="result_value", measurement_name="BMI", match_type="startswith", output_file="bmi.parquet")
    extract_measurement(input_file="omr.parquet", name_col="result_name", value_col="result_value", measurement_name="Blood Pressure", match_type="startswith", output_file="bp.parquet")
    expand_feature(input_file="bp.parquet", feature="Blood Pressure", expand_on="/", output_file="bp.parquet")         
    
    merge_data(input_file_base="cohort.parquet", input_file_merge="prescriptions.parquet", output_file="merged_data.parquet", merge_on="hadm_id", how="left")  
    merge_data(input_file_base="merged_data.parquet", input_file_merge="diagnoses.parquet", output_file="merged_data.parquet", merge_on="hadm_id", how="left") 
    merge_data(input_file_base="merged_data.parquet", input_file_merge="admissions.parquet", output_file="merged_data.parquet", merge_on="hadm_id", how="left")        
    merge_data(input_file_base="merged_data.parquet", input_file_merge="patients.parquet", output_file="merged_data.parquet", merge_on="subject_id", how="left") 
    
    merge_on_date(input_file_base="merged_data.parquet", date_col_base="admittime", input_file_merge="bmi.parquet", date_col_merge="chartdate", output_file="merged_data.parquet", merge_on="subject_id", direction="nearest")  
    merge_on_date(input_file_base="merged_data.parquet", date_col_base="admittime", input_file_merge="bp.parquet", date_col_merge="chartdate", output_file="merged_data.parquet", merge_on="subject_id", direction="nearest")
    
    feature_selection(input_file = "merged_data.parquet", output_file = "merged_data.parquet", columns = "anchor_age,gender,BMI,Blood Pressure_0,Blood Pressure_1,n_medications,n_diagnoses,hospital_expire_flag")
        
    drop_nan(input_file = "merged_data.parquet", output_file = "cleaned_real_data.parquet")

    copula_synthesizer(input_file = "cleaned_real_data.parquet", output_file = "synthetic_data.parquet")
    #vae_synthesizer(input_file = "cleaned_real_data.parquet", output_file = "synthetic_data.parquet")
    #ctgan_synthesizer(input_file = "cleaned_real_data.parquet", output_file = "synthetic_data.parquet")

    statistical_evaluation(real_data_file="cleaned_real_data.parquet", synthetic_data_file="synthetic_data.parquet", output_file="statistical_evaluation.parquet")

    label_encoder(input_file = "cleaned_real_data.parquet", columns = "gender", output_file = "cleaned_real_data.parquet")
    label_encoder(input_file = "synthetic_data.parquet", columns = "gender", output_file = "synthetic_data.parquet")

    privacy_evaluation(real_data_file="cleaned_real_data.parquet", synthetic_data_file="synthetic_data.parquet", output_file="privacy_evaluation.parquet")

    utility_evaluation(real_data_file="cleaned_real_data.parquet", synthetic_data_file="synthetic_data.parquet", target_col="hospital_expire_flag", train_size=0.7, model_name="SVC", metrics="accuracy, f1, roc_auc",output_file="utility_evaluation.parquet")

if __name__ == "__main__":
    sydgep_pipeline()
