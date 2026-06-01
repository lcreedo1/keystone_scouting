import os
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import DeadlineExceeded
import pandas as pd
import json

class BigQueryConnector:
    def __init__(self, project_id: str):
        self.project_id = project_id
        self.client = bigquery.Client(project=self.project_id)
        
    def _ensure_dataset_exists(self, dataset_name: str):
        """
        Checks if a dataset exists, and creates it if it doesn't.
        """
        dataset_ref = bigquery.DatasetReference(self.project_id, dataset_name)
        
        try:
            self.client.get_dataset(dataset_ref)
            # print(f"Dataset '{dataset_name}' verified.")
        except NotFound:
            print(f"✨ Dataset '{dataset_name}' not found. Creating it now...")
            # Create a blank dataset object
            dataset = bigquery.Dataset(dataset_ref)
            
            # Set your physical data location (US is standard, or use US-West4 / US-Central1)
            dataset.location = "US" 
            
            self.client.create_dataset(dataset, timeout=10)
            print(f"✅ Dataset '{dataset_name}' successfully created in location: {dataset.location}")

    def upload_dataframe(self, df: pd.DataFrame, dataset_name: str, table_name: str, append: bool = False):
        self._ensure_dataset_exists(dataset_name)
        target_table = f"{self.project_id}.{dataset_name}.{table_name}"
        
        df_cleaned = df.copy()
    
        # Optimized Step 2: Stringify objects quickly
        for col in df_cleaned.columns:
            # Only inspect columns that Pandas flags as generic Python objects
            if df_cleaned[col].dtype == "object":
                # Select a sample row to check if it's a dict or list without iterating everything
                first_valid = df_cleaned[col].dropna().iloc[0] if not df_cleaned[col].dropna().empty else None
                
                if isinstance(first_valid, (dict, list)):
                    print(f"Baking complex object column down to string format: {col}")
                    # Stringify lists/dicts via JSON; map handles missing/null values cleanly
                    df_cleaned[col] = df_cleaned[col].map(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)
                
                # Cast the entire column cleanly to string for PyArrow compatibility
                df_cleaned[col] = df_cleaned[col].astype(str)
                
        # 3. Handle standard BigQuery column sanitization
        df_cleaned.columns = df_cleaned.columns.str.replace(" ", "_").str.replace("/", "_").str.replace("-", "_")

        # Check if the table already exists when appending
        if append:
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                autodetect=False
            )
        else:
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",
                autodetect=True   # Only auto-detect on the initial table creation
            )
        
        print(f"🚀 Streaming via your personal account to {target_table}...")
        job = self.client.load_table_from_dataframe(df_cleaned, target_table, job_config=job_config)
        print(f"📬 Job tracking ID: {job.job_id}. Processing in background...")