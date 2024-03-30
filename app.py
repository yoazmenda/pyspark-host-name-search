import os
import subprocess
from pyspark.sql import SparkSession
from tqdm import tqdm

# Constants
LOCAL_ZIP_PATH = "./data/letsencrypt_oak2022.7z"
LOCAL_TXT_PATH = "./data/letsencrypt_oak2022.txt"
SEARCH_EMAIL = "www.phantasmaphile.com"

def extract_file(zip_path, extract_to):
    try:
        subprocess.run(["7z", "e", zip_path, f"-o{extract_to}", "-y"], check=True)
        print("Extraction complete.")
    except subprocess.CalledProcessError as e:
        print("Failed to extract file:", e)
        exit(1)

def init_spark_session():
    return SparkSession.builder \
        .master("local[*]") \
        .appName("LineCounterAndEmailFinder") \
        .getOrCreate()

def process_file_with_spark(spark, file_path, search_email):
    df = spark.read.text(file_path)
    
    # Counting lines with a progress indicator
    print("Counting lines...")
    line_count = df.count()
    print(f"Total number of lines: {line_count}")

    # Searching for email
    print(f"Searching for occurrences of '{search_email}'...")
    matches = df.filter(df.value.contains(search_email)).collect()
    
    if matches:
        print(f"Found {len(matches)} occurrences of {search_email}:")
        for match in matches:
            print(match[0])
    else:
        print(f"No occurrences of {search_email} found.")

if __name__ == "__main__":
    if not os.path.exists(LOCAL_TXT_PATH):
        print("Extracting file...")
        # Ensure the output directory exists
        os.makedirs(os.path.dirname(LOCAL_TXT_PATH), exist_ok=True)
        extract_file(LOCAL_ZIP_PATH, os.path.dirname(LOCAL_TXT_PATH))
    else:
        print("File already extracted.")

    print("Initializing Spark session...")
    spark = init_spark_session()

    print("Processing file with Spark...")
    process_file_with_spark(spark, LOCAL_TXT_PATH, SEARCH_EMAIL)

    spark.stop()

