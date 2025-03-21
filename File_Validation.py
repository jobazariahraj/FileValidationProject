import pandas as pd
import logging
import os

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("FileIngestionValidation")

# Configurations for source and target (local paths or accessible mount points)
config = {
    "source_path": "/Users/jobazariahraj/Downloads/Entity_DataSet.csv",  # Local or accessible source file
    "target_path": "/Users/jobazariahraj/Downloads/Target_File.csv",  # Local target file
    "discrepancy_report": "/Users/jobazariahraj/Downloads/discrepancy_report.csv",  # Discrepancy report file path
    "no_discrepancy_report": "/Users/jobazariahraj/Downloads/no_discrepancy_report.csv",  # No discrepancy report file path
    "mismatch_records": "/Users/jobazariahraj/Downloads/mismatch_records.csv"  # Mismatch records file path
}

def file_exists(path):
    return os.path.isfile(path)

try:
    if not file_exists(config["source_path"]):
        raise FileNotFoundError(f"Source file not found at {config['source_path']}")

    # Load source file
    source_df = pd.read_csv(config["source_path"])
    logger.info("✅ Source file loaded successfully.")

    # If target doesn't exist, create it from source
    if not file_exists(config["target_path"]):
        source_df.to_csv(config["target_path"], index=False)
        logger.info("✅ File successfully written to target.")

    # Reload target file to validate
    target_df = pd.read_csv(config["target_path"])
    logger.info("✅ Target file loaded successfully. Validation process started...")

    # Standardize column names (lowercase, underscores)
    source_df.columns = [c.lower().replace(" ", "_") for c in source_df.columns]
    target_df.columns = [c.lower().replace(" ", "_") for c in target_df.columns]

    # Find common and missing columns
    source_cols = set(source_df.columns)
    target_cols = set(target_df.columns)

    common_columns = list(source_cols & target_cols)
    missing_in_target = list(source_cols - target_cols)
    missing_in_source = list(target_cols - source_cols)

    if missing_in_target or missing_in_source:
        logger.warning(f"Columns missing in target: {missing_in_target}")
        logger.warning(f"Columns missing in source: {missing_in_source}")

    schema_mismatch = missing_in_target + missing_in_source

    # Row count validation
    source_row_count = len(source_df)
    target_row_count = len(target_df)
    row_count_mismatch = source_row_count != target_row_count

    # Column count validation
    column_count_mismatch = len(source_df.columns) != len(target_df.columns)

    # Calculate missing rows in target (based on common columns)
    rows_missing_in_target = pd.merge(source_df[common_columns], target_df[common_columns],
                                      how='left', indicator=True).query('_merge == "left_only"').drop(columns=['_merge'])
    missing_count = len(rows_missing_in_target)

    # Duplicate handling in target
    duplicate_count = target_df.duplicated(subset=common_columns, keep=False).sum()
    has_duplicates = duplicate_count > 0

    # Save mismatched records
    if missing_count > 0:
        rows_missing_in_target.to_csv(config["mismatch_records"], index=False)
        logger.info(f"🚨 Mismatched records saved at: {config['mismatch_records']}")

    # Generate discrepancy report
    if schema_mismatch or row_count_mismatch or column_count_mismatch or missing_count > 0 or has_duplicates:
        logger.warning("⚠️ Discrepancies found. Generating report...")

        discrepancy_data = [
            {"Validation": "Schema Mismatch", "Discrepancy": ", ".join(schema_mismatch) if schema_mismatch else "No Issue"},
            {"Validation": "Row Count Mismatch", "Discrepancy": f"Source: {source_row_count}, Target: {target_row_count}" if row_count_mismatch else "No Issue"},
            {"Validation": "Column Count Mismatch", "Discrepancy": f"Source: {len(source_df.columns)}, Target: {len(target_df.columns)}" if column_count_mismatch else "No Issue"},
            {"Validation": "Source Minus Target", "Discrepancy": f"Rows missing in Target: {missing_count}" if missing_count > 0 else "No Issue"},
            {"Validation": "Duplicate Rows in Target", "Discrepancy": f"Duplicate count: {duplicate_count}" if has_duplicates else "No Issue"}
        ]

        discrepancy_df = pd.DataFrame(discrepancy_data)
        discrepancy_df.to_csv(config["discrepancy_report"], index=False)
        logger.info(f"🚨 Discrepancy report saved at: {config['discrepancy_report']}")
    else:
        logger.info("✅ No discrepancies found. Data is consistent.")
        no_discrepancy_df = pd.DataFrame([{ "Validation": "No Discrepancy", "Message": "No discrepancies found. Data is consistent." }])
        no_discrepancy_df.to_csv(config["no_discrepancy_report"], index=False)
        logger.info(f"✅ No discrepancy report saved at: {config['no_discrepancy_report']}")

except FileNotFoundError as e:
    logger.error(f"File Not Found: {str(e)}")
except Exception as e:
    logger.error(f"Unexpected error: {str(e)}")
