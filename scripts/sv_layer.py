import sys
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, datediff, col, to_date, lit, pow, round, when, last

raw_input = "/app/data/raw_files/"
bronze_path = "/app/data/bronze"
database_output = "/app/database/wallet.db" 

def read_data(spark):
    
    try:
        # Read the Parquet file
        df_bronze = spark.read.parquet(bronze_path)    
        print("Successful Parquet read!")
    
    except Exception as e:
        print(f"Error reading data from SQLite: {e}")

    try:
        json_files = [os.path.join(raw_input, f) for f in os.listdir(raw_input) if f.endswith(".json") and "cdi" in f]
        df_cdi = spark.read.option("multiLine", True).json(json_files)

    except Exception as e:
        print(f"Error reading data from SQLite: {e}")
    
    return df_cdi, df_bronze

def save_table(df):
    try:   
        df.write \
            .format("jdbc") \
            .option("url", f"jdbc:sqlite:{database_output}") \
            .option("dbtable", "wallet_bonus_cdi") \
            .mode("overwrite") \
            .save()
        
        print("Data successfully written to the 'wallet_bonus_cdi' table in SQLite!") 
        print("--- Silver Layer Complete ---")  
    except Exception as e:
        print(f"Error writing data to SQLite: {e}") 

def transform_data(df_cdi, df_bronze):
    
    # CDI
    # Daily CDI rate calculation
    calc_cdi_daily = (pow((lit(1) + col('cdi_rate_annual')/100), lit(1)/lit(252)) - lit(1))
    df_cdi_daily = df_cdi.withColumn('cdi_rate_daily', calc_cdi_daily)\
                         .withColumn("date", to_date("cdi_date"))
    
    # Get the last transaction of the user per day
    df_last_transaction = df_bronze.groupBy("user_id", "date").agg(
                                            last("transaction_date").alias("transaction_date"),
                                            last("transaction_type").alias("transaction_type"),
                                            last("amount").alias("amount"),
                                            last("balance_before").alias("balance_before"),
                                            last("balance_after").alias("balance_after")
                                            )
    
    # Construct the wallet movement window
    window_spec = Window.partitionBy("user_id").orderBy("date")
    df_wallet_without_movement = df_last_transaction.withColumn("last_date", lag("date").over(window_spec))\
                                           .withColumn("days_without_movement", when(datediff("date", "last_date").isNotNull(), datediff("date", "last_date")).otherwise(lit(0)))\
                                           .withColumn("is_eligible", (col("balance_after") > 100) & (col("days_without_movement") >= 1))
    
    # The lag function processes all partitions, which can be slow with large datasets. So I use cache before using the window function
    df_wallet_without_movement.cache()

    df_silver = df_wallet_without_movement.join(df_cdi_daily, on="date", how="left")\
                           .withColumn("cdi_bonus", when(col('is_eligible'), col("balance_after") * col("cdi_rate_daily")).otherwise(lit(None)))\
                           .withColumn("balance_new", when(col('is_eligible') , round(col("balance_after") + col("cdi_bonus"), 2)).otherwise(col('balance_after')))

    df_silver = df_silver.select("user_id", 
                                "date", 
                                col("balance_after").alias('balance'),
                                "days_without_movement",
                                "is_eligible",
                                "cdi_rate_annual", 
                                "cdi_rate_daily", 
                                col("cdi_bonus").alias('amount_cdi_bonus'), 
                                "balance_new")
    
    return df_silver
     
if __name__ == "__main__":
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    try:
        spark = SparkSession.builder \
                            .appName("SilverLayer") \
                            .getOrCreate()

        # Silver Layer: History Construction
        dfs = read_data(spark)
        df_cdi = dfs[0]
        df_bronze = dfs[1]
 
        df_silver = transform_data(df_cdi, df_bronze)
        df_silver.show()                                     
        
        save_table(df_silver)
    
    except Exception as e:
        logging.error(f"\nERROR: The script workflow failed: {e}")
        sys.exit(1)

    finally:
        spark.stop()
        logging.info("SparkSession terminated.")