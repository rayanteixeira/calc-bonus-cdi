import sys
import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col


raw_input = "/app/data/raw_files/"
bronze_path = "/app/data/bronze"

# Bronze Layer: CDC Data Ingestion
def transform_data(df_cdc):
    df_bronze = df_cdc.withColumn("date", to_date(col("transaction_date").substr(1, 10), "yyyy-MM-dd"))
    return df_bronze      

def save_table(df):
    try:   
        df.write.mode("overwrite").parquet(bronze_path)
        logging.info("Data successfully written to the bronze")
        logging.info("--- Bronze Layer Complete ---")
    except Exception as e:
        logging.error(f"Error writing data to Parquet: {e}") 
   
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    try:
        spark = SparkSession.builder.appName("BronzeLayer").getOrCreate()

        # Check if the input directory exists
        if not os.path.exists(raw_input):
            logging.error(f"Error: The directory {raw_input} was not found!")
            sys.exit(1)

        # List JSON files
        json_files = [os.path.join(raw_input, f) for f in os.listdir(raw_input) if f.endswith(".json") and "transactions" in f]
        
        if not json_files:
            logging.warning(f"No JSON files found in {raw_input}")
            sys.exit(1)

        # Read JSON transaction files
        df_cdc = spark.read.option("multiLine", True).json(json_files)
            
        df_bronze = transform_data(df_cdc)
        df_bronze.orderBy('user_id', col('transaction_date').asc())\
                 .show(1000)
        
        save_table(df_bronze)
        
    except Exception as e:
        logging.error(f"\nERROR: The script workflow failed: {e}")
        sys.exit(1) 
        
    finally:
        spark.stop()
        logging.info("SparkSession terminated.")
