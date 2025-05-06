import pandas as pd
import matplotlib.pyplot as plt
from prefect import task, flow, get_run_logger

@task
def fetch_data(dataset_path: str) -> pd.DataFrame:
    logger = get_run_logger()
    logger.info(f"Reading data from {dataset_path}")
    
    df = pd.read_csv(dataset_path)
    logger.info(f"Data shape: {df.shape}")
    return df

@task
def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()
    logger.info("Validating data")
    
    missing_values = df.isnull().sum()
    logger.info(f"Missing values:\n{missing_values}")
    
    df_clean = df.dropna()
    return df_clean

@task
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()
    logger.info("Transforming data")
    
    if "sales" in df.columns:
        df["sales_normalized"] = (df["sales"] - df["sales"].mean()) / df["sales"].std()
    
    return df

@task
def generate_analytics_report(df: pd.DataFrame, output_path: str):
    logger = get_run_logger()
    logger.info("Generating analytics report")
    
    summary = df.describe()
    summary.to_csv(output_path)
    
    logger.info(f"Summary statistics saved to {output_path}")

@task
def create_sales_histogram(df: pd.DataFrame, output_path: str):
    logger = get_run_logger()
    
    if "sales" in df.columns:
        logger.info("Creating sales histogram")
        plt.hist(df["sales"], bins=20)
        plt.title("Sales Distribution")
        plt.xlabel("Sales")
        plt.ylabel("Frequency")
        plt.savefig(output_path)
        plt.close()
        logger.info(f"Sales histogram saved to {output_path}")

@flow
def analytics_pipeline(dataset_path: str, summary_output: str, histogram_output: str):
    logger = get_run_logger()
    logger.info("Starting Analytics Pipeline")
    
    df = fetch_data(dataset_path)
    df_clean = validate_data(df)
    df_transformed = transform_data(df_clean)
    
    generate_analytics_report(df_transformed, summary_output)
    create_sales_histogram(df_transformed, histogram_output)
    
    logger.info("Analytics Pipeline completed")

if __name__ == "__main__":
    analytics_pipeline(
        dataset_path="C:/Users/AliRazaHaider/Desktop/de/DE/lab6/analytics_data.csv",
        summary_output="C:/Users/AliRazaHaider/Desktop/de/DE/lab6/analytics_summary.csv",
        histogram_output="C:/Users/AliRazaHaider/Desktop/de/DE/lab6/sales_histogram.png"
    )



