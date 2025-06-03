# Imports
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType
from datetime import datetime
from pyspark.sql.functions import to_date
import sys

DB_PATH_OUT = "/app/database/bronze_layer.db" 

def generate_cdc(spark):
    
    #------------------
    # EXAMPLE CDC
    #------------------
    schema_cdc = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("transaction_date", TimestampType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("balance_before", DoubleType(), True),
        StructField("balance_after", DoubleType(), True),
    ])

    data_cdc = [
        ("txn_0001", "user_001", datetime(2025, 5, 28, 10, 30), "Deposit", 200.00, 1500.00, 1700.00),
        ("txn_2111", "user_002", datetime(2025, 5, 28, 12, 10), "Deposit", 150.00, 900.00, 1050.00),
        ("txn_1232", "user_003", datetime(2025, 5, 28, 14, 45), "Payment", 100.00, 1200.00, 1100.00),
        ("txn_3202", "user_001", datetime(2025, 5, 29, 16, 30), "Deposit", 250.00, 1700.00, 1950.00),
        ("txn_5603", "user_004", datetime(2025, 6, 1, 9, 10), "Transfer", 300.00, 2500.00, 2200.00),
        ("txn_0475", "user_002", datetime(2025, 5, 30, 17, 55), "Transfer", 500.00, 1050.00, 550.00),
        ("txn_6653", "user_005", datetime(2025, 6, 2, 10, 20), "Payment", 50.00, 75.00, 25.00),
        ("txn_6422", "user_003", datetime(2025, 5, 29, 18, 40), "Payment", 100.00, 1100.00, 1000.00),
        ("txn_6434", "user_001", datetime(2025, 6, 1, 11, 55), "Transfer", 75.00, 1950.00, 1875.00),
        ("txn_6343", "user_001", datetime(2025, 6, 2, 12, 15), "Transfer", 55.00, 1875.00, 1820.00),
        ("txn_6555", "user_004", datetime(2025, 6, 2, 15, 25), "Transfer", 200.00, 2200.00, 2000.00),
        ("txn_8623", "user_005", datetime(2025, 6, 2, 16, 20), "Deposit", 120.00, 25.00, 145.00),
    ]

    df_cdc = spark.createDataFrame(data=data_cdc, schema=schema_cdc)

    
    return df_cdc

# Bronze Layer: CDC Data Ingestion
def transform_data(df_cdc):

        df_bronze = df_cdc.withColumn("date", to_date("transaction_date"))

        return df_bronze      

def save_table(df):
    try:   
        df.write \
            .format("jdbc") \
            .option("url", f"jdbc:sqlite:{DB_PATH_OUT}") \
            .option("dbtable", "bz_wallet") \
            .mode("overwrite") \
            .save()
        print("Data successfully written to the 'bz_wallet' table in SQLite!")    
    except Exception as e:
        print(f"Error writing data to SQLite: {e}") 
   
        
if __name__ == "__main__":
    
    try:
        spark = SparkSession.builder \
                            .appName("BronzeLayer") \
                            .getOrCreate()

        df_cdc = generate_cdc(spark)
        
        df_bronze = transform_data(df_cdc)
        
        save_table(df_bronze)
        print("--- Bronze Layer Complete ---")
    
    except Exception as e:
        print(f"\nERROR: The script workflow failed: {e}")
        sys.exit(1) 
        
    finally:
        spark.stop()
        print("SparkSession terminated.")
