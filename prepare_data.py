import zipfile
import os
import polars as pl


# Specify the name of the self-extracting ZIP file
self_extracting_zip = "sample-sales.zip"

# Specify the directory where you want to extract the files
extraction_directory = "data"

# Define the extraction logic
def extract_zip():
    try:
        # Create the extraction directory if it doesn't exist
        os.makedirs(extraction_directory, exist_ok=True)

        # Extract the ZIP contents
        with zipfile.ZipFile(self_extracting_zip, "r") as zip_ref:
            zip_ref.extractall(extraction_directory)

        print("Extraction complete.")
    except Exception as e:
        print(f"Extraction failed: {e}")


# Check if this script is the main script being run
if __name__ == "__main__":
    # Check if the self-extracting script is being run from within the ZIP archive
    # This is the entry point when running the self-extracting script
    print("Self-extracting archive...")
    print(f"Extracting to: {os.path.abspath(extraction_directory)}")
    extract_zip()
    df = pl.scan_csv("data/*.csv").collect()
    print("Create the DataFrame with 50k rows")
    df.slice(0, 50000).write_csv(f"{extraction_directory}/sales_50000.csv")
    print("Successfully created sales_50000")
    print("Create the DataFrame with 250k rows")

    # Create the second DataFrame with 250k rows
    df2 = df.slice(0, 250000).write_csv(f"{extraction_directory}/sales_250000.csv")
    print("Successfully created sales_250000")

    print("Create the DataFrame with 1M rows")

    # Create the third DataFrame with 1M rows
    df3 = df.slice(0, 1000000).write_csv(f"{extraction_directory}/sales_1000000.csv")
    print("Successfully created sales_1000000")

    print("Create the DataFrame with 25M rows")

    bigger_df = pl.concat([df, df, df, df, df]).write_csv(f"{extraction_directory}/sales_25000000.csv")
    print("Successfully created sales_25000000")

