import os
import pandas as pd
import dlt

CSV_FILES = {
    "crm_cust_info":    "Datasets/source_crm/cust_info.csv",
    "crm_prd_info":     "Datasets/source_crm/prd_info.csv",
    "crm_sales_details":"Datasets/source_crm/sales_details.csv",
    "erp_cust_az12":    "Datasets/source_erp/CUST_AZ12.csv",
    "erp_loc_a101":     "Datasets/source_erp/LOC_A101.csv",
    "erp_px_cat_g1v2":  "Datasets/source_erp/PX_CAT_G1V2.csv",
}

def read_csvs(csv_files):
    """Read CSV files into pandas DataFrames."""
    data = {}
    for table_name, file_path in csv_files.items():
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        data[table_name] = pd.read_csv(file_path)
    return data

def main():
    # Step 1: Read all CSV files
    data = read_csvs(CSV_FILES)

    # Step 2: Create a dlt pipeline for PostgreSQL
    pipeline = dlt.pipeline(
        pipeline_name="csv_to_postgres_pipeline",
        destination="postgres",
        dataset_name="bronze"
    )

    # Step 3: Load each DataFrame to its own table in PostgreSQL
    for table_name, df in data.items():
        print(f"Loading '{table_name}' into PostgreSQL...")
        load_info = pipeline.run(
            df.to_dict(orient="records"),
            table_name=table_name,
            write_disposition="replace"
        )
        print(f"Loaded {load_info.loads_ids[0]} ({table_name})")

    print("All CSV files loaded into PostgreSQL successfully!")

if __name__ == "__main__":
    main()
