# Imports
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, datediff
import sys

DB_PATH_IN = "/app/database/bronze_layer.db"
DB_PATH_OUT = "/app/database/silver_layer.db" 

def read_data(spark):
    
    try:
        df_bronze = spark.read \
            .format("jdbc") \
            .option("url", f"jdbc:sqlite:{DB_PATH_IN}") \
            .option("dbtable", "bz_wallet") \
            .load()
            
        print("\nData read from SQLite:")
        print("Successful SQLite read!")
        
        return df_bronze
    except Exception as e:
        print(f"Error reading data from SQLite: {e}")

def save_table(df):
    try:   
        df.write \
            .format("jdbc") \
            .option("url", f"jdbc:sqlite:{DB_PATH_OUT}") \
            .option("dbtable", "sv_wallet_history") \
            .mode("overwrite") \
            .save()
        print("Data successfully written to the 'sv_wallet_history' table in SQLite!")    
    except Exception as e:
        print(f"Error writing data to SQLite: {e}") 

def transform_data(df_bronze):
    window_spec = Window.partitionBy("user_id").orderBy("date")
    df_silver = df_bronze.withColumn("last_date", lag("date").over(window_spec))\
                         .withColumn("days_without_movement", datediff("date", "last_date")) 
    return df_silver          
if __name__ == "__main__":
    
    try:
        spark = SparkSession.builder \
                            .appName("SilverLayer") \
                            .getOrCreate()

        # Silver Layer: History Construction
        df_bronze = read_data(spark)
        
        df_silver = transform_data(df_bronze)
                                                
        save_table(df_silver)
    
        print("--- Silver Layer Complete ---")
    
    except Exception as e:
        print(f"\nERROR: The script workflow failed: {e}")
        sys.exit(1)

    finally:
        spark.stop()
        print("SparkSession terminated.")
