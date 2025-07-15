import os
import pandas as pd
import dlt
from typing import Iterator, Dict, Any

# CSV file mappings
CSV_FILES = {
    "crm_cust_info": "seeds/crm/cust_info.csv",
    "crm_prd_info": "seeds/crm/prd_info.csv",
    "crm_sales_details": "seeds/crm/sales_details.csv",
    "erp_cust_az12": "seeds/erp/CUST_AZ12.csv",
    "erp_loc_a101": "seeds/erp/LOC_A101.csv",
    "erp_px_cat_g1v2": "seeds/erp/PX_CAT_G1V2.csv",
}

# DLT Resource Definitions with Schema
@dlt.resource(
    name="crm_cust_info",
    write_disposition="replace",
    columns={
        "cst_id": {"data_type": "bigint"},
        "cst_key": {"data_type": "text"},
        "cst_firstname": {"data_type": "text"},
        "cst_lastname": {"data_type": "text"},
        "cst_marital_status": {"data_type": "text"},
        "cst_gndr": {"data_type": "text"},
        "cst_create_date": {"data_type": "date"},
    }
)
def load_crm_cust_info() -> Iterator[Dict[str, Any]]:
    """Load CRM customer information from CSV"""
    file_path = CSV_FILES["crm_cust_info"]
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    df = pd.read_csv(file_path)
    yield from df.to_dict('records')

@dlt.resource(
    name="crm_prd_info",
    write_disposition="replace",
    columns={
        "prd_id": {"data_type": "bigint"},
        "cat_id": {"data_type": "text"},
        "prd_key": {"data_type": "text"},
        "prd_nm": {"data_type": "text"},
        "prd_cost": {"data_type": "bigint"},
        "prd_line": {"data_type": "text"},
        "prd_start_dt": {"data_type": "date"},
        "prd_end_dt": {"data_type": "date"},
    }
)
def load_crm_prd_info() -> Iterator[Dict[str, Any]]:
    """Load CRM product information from CSV"""
    file_path = CSV_FILES["crm_prd_info"]
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    df = pd.read_csv(file_path)
    yield from df.to_dict('records')

@dlt.resource(
    name="crm_sales_details",
    write_disposition="replace",
    columns={
        "sls_ord_num": {"data_type": "text"},
        "sls_prd_key": {"data_type": "text"},
        "sls_cust_id": {"data_type": "bigint"},
        "sls_order_dt": {"data_type": "bigint"},
        "sls_ship_dt": {"data_type": "bigint"},
        "sls_due_dt": {"data_type": "bigint"},
        "sls_sales": {"data_type": "bigint"},
        "sls_quantity": {"data_type": "bigint"},
        "sls_price": {"data_type": "bigint"},
    }
)
def load_crm_sales_details() -> Iterator[Dict[str, Any]]:
    """Load CRM sales details from CSV"""
    file_path = CSV_FILES["crm_sales_details"]
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    df = pd.read_csv(file_path)
    yield from df.to_dict('records')

@dlt.resource(
    name="erp_cust_az12",
    write_disposition="replace",
    columns={
        "cid": {"data_type": "text"},
        "bdate": {"data_type": "date"},
        "gen": {"data_type": "text"},
    }
)
def load_erp_cust_az12() -> Iterator[Dict[str, Any]]:
    """Load ERP customer data from CSV"""
    file_path = CSV_FILES["erp_cust_az12"]
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    df = pd.read_csv(file_path)
    yield from df.to_dict('records')

@dlt.resource(
    name="erp_loc_a101",
    write_disposition="replace",
    columns={
        "cid": {"data_type": "text"},
        "cntry": {"data_type": "text"},
    }
)
def load_erp_loc_a101() -> Iterator[Dict[str, Any]]:
    """Load ERP location data from CSV"""
    file_path = CSV_FILES["erp_loc_a101"]
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    df = pd.read_csv(file_path)
    yield from df.to_dict('records')

@dlt.resource(
    name="erp_px_cat_g1v2",
    write_disposition="replace",
    columns={
        "id": {"data_type": "text"},
        "cat": {"data_type": "text"},
        "subcat": {"data_type": "text"},
        "maintenance": {"data_type": "text"},
    }
)
def load_erp_px_cat_g1v2() -> Iterator[Dict[str, Any]]:
    """Load ERP product category data from CSV"""
    file_path = CSV_FILES["erp_px_cat_g1v2"]
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    df = pd.read_csv(file_path)
    yield from df.to_dict('records')

@dlt.source(name="csv_data_source")
def csv_data_pipeline():
    """Main DLT source combining all CSV resources"""
    return [
        load_crm_cust_info(),
        load_crm_prd_info(),
        load_crm_sales_details(),
        load_erp_cust_az12(),
        load_erp_loc_a101(),
        load_erp_px_cat_g1v2()
    ]

def main():
    """Main pipeline execution function"""
    print("Starting CSV to PostgreSQL pipeline with DLT schema...")

    # Create DLT pipeline
    pipeline = dlt.pipeline(
        pipeline_name="csv_to_postgres_pipeline",
        destination="postgres",
        dataset_name="bronze",
        refresh="drop_sources",
        progress="log"
    )

    # Load data using the source
    print("Loading all CSV files...")
    pipeline.run(csv_data_pipeline())

    # Print load results
    print(f"Pipeline completed successfully!")

if __name__ == "__main__":
    main()
