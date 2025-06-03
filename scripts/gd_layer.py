# Imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, lit, pow, round
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import sys

DB_PATH_IN = "/app/database/silver_layer.db"
DB_PATH_OUT = "/app/database/gold_layer.db" 

def generate_cdi(spark):
    #------------------
    # EXAMPLE CDI
    #------------------
    # Define the table schema
    schema_cdi = StructType([
        StructField("cdi_date", TimestampType(), True),
        StructField("cdi_rate_annual", StringType(), True)
    ])

    # Fixed data
    data_cdi = [
        (datetime(2025, 5, 28, 0, 0, 0), "14.65"),
        (datetime(2025, 5, 29, 0, 0, 0), "10.23"),
        (datetime(2025, 5, 30, 0, 0, 0), "10.23"),
        (datetime(2025, 6, 2, 0, 0, 0), "13.03")
    ]

    # Create DataFrame CDI
    df_cdi = spark.createDataFrame(data=data_cdi, schema=schema_cdi)

    return df_cdi

def read_data(spark):
    
    try:
        df_silver = spark.read \
            .format("jdbc") \
            .option("url", f"jdbc:sqlite:{DB_PATH_IN}") \
            .option("dbtable", "sv_wallet_history") \
            .load()
            
        print("\nData read from SQLite:")
        print("Successful SQLite read!")
        
        return df_silver
    except Exception as e:
        print(f"Error reading data from SQLite: {e}")

def save_table(df):
    try:   
        df.write \
            .format("jdbc") \
            .option("url", f"jdbc:sqlite:{DB_PATH_OUT}") \
            .option("dbtable", "gd_wallet_cdi") \
            .mode("overwrite") \
            .save()
        print("Data successfully written to the 'gd_wallet_cdi' table in SQLite!")    
    except Exception as e:
        print(f"Error writing data to SQLite: {e}") 

def transform_data(df_cdi, df_silver):

    calc_cdi_daily = (pow((lit(1) + col('cdi_rate_annual')/100), lit(1)/lit(252)) - lit(1))
    df_cdi_daily = df_cdi.withColumn('cdi_rate_daily', calc_cdi_daily)\
                        .withColumn("date", to_date("cdi_date"))

    df_eligible = df_silver.filter((col("balance_after") > 100) & (col("days_without_movement") >= 1))

    df_movement_eligible = df_eligible.join(df_cdi_daily, on="date", how="inner")

    df_gold = df_movement_eligible.withColumn("cdi_bonus", col("balance_after") * col("cdi_rate_daily"))\
                                .withColumn("balance_new", round(col("balance_after") + col("cdi_bonus"), 2))\
                                .select("user_id", 
                                        "date", 
                                        "balance_after", 
                                        "cdi_rate_annual", 
                                        "cdi_rate_daily", 
                                        "cdi_bonus", 
                                        "balance_new")
    
    return df_gold
if __name__ == "__main__":
    
    try:
        spark = SparkSession.builder \
                            .appName("GoldLayer") \
                            .getOrCreate()

        df_cdi = generate_cdi(spark)
        df_silver = read_data(spark)
        
        # CDI daily rate calculation
        df_gold = transform_data(df_cdi, df_silver)
        df_gold.orderBy(col('user_id').asc(), col('date').asc()).show()                                       
        save_table(df_gold)
    
        print("--- Gold Layer Complete ---")
    except Exception as e:
        print(f"\nERROR: The script workflow failed: {e}")
        sys.exit(1) 

    finally:
        spark.stop()
        print("SparkSession terminated.")
