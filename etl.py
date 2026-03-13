from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, sum as _sum, count, when
from pyspark.sql.types import IntegerType, DoubleType
import os

def create_spark_session():
    return SparkSession.builder \
        .appName("ECommerce_ETL") \
        .getOrCreate()

def extract_data(spark, raw_dir):
    print("Extracting data...")
    customers_df = spark.read.csv(os.path.join(raw_dir, "customers.csv"), header=True, inferSchema=True)
    products_df = spark.read.csv(os.path.join(raw_dir, "products.csv"), header=True, inferSchema=True)
    orders_df = spark.read.csv(os.path.join(raw_dir, "orders.csv"), header=True, inferSchema=True)
    reviews_df = spark.read.csv(os.path.join(raw_dir, "reviews.csv"), header=True, inferSchema=True)
    return customers_df, products_df, orders_df, reviews_df

def transform_data(customers_df, products_df, orders_df, reviews_df):
    print("Transforming data...")
    
    # 1. Clean missing values in orders (discount_code)
    # Replaced missing discount codes with 'NONE'
    orders_df = orders_df.fillna({"discount_code": "NONE"})
    
    # Cast date columns
    orders_df = orders_df.withColumn("order_date", to_date(col("order_date")))
    customers_df = customers_df.withColumn("registration_date", to_date(col("registration_date")))
    
    # 2. Fact Orders: Join Orders with Customers and Products
    fact_orders_df = orders_df.alias("o") \
        .join(customers_df.alias("c"), col("o.customer_id") == col("c.customer_id"), "left") \
        .join(products_df.alias("p"), col("o.product_id") == col("p.product_id"), "left") \
        .select(
            col("o.order_id"),
            col("o.customer_id"),
            col("o.product_id"),
            col("o.order_date"),
            col("o.quantity"),
            col("o.total_amount"),
            col("o.discount_code"),
            col("o.status"),
            col("c.state").alias("customer_state"),
            col("p.category").alias("product_category"),
            col("p.cost").alias("product_cost")
        )
    
    # Calculate profit margin per order
    fact_orders_df = fact_orders_df.withColumn("profit", col("total_amount") - (col("product_cost") * col("quantity")))
    
    # 3. Aggregate daily sales summary
    daily_sales_df = fact_orders_df \
        .groupBy("order_date") \
        .agg(
            _sum("total_amount").alias("revenue"),
            _sum("profit").alias("total_profit"),
            count("order_id").alias("number_of_orders")
        ) \
        .orderBy("order_date")
    
    return fact_orders_df, daily_sales_df

def load_data(fact_orders_df, daily_sales_df):
    import os
    from dotenv import load_dotenv
    
    # Load environment variables from .env file
    load_dotenv()
    
    sf_options = {
        "sfURL": f"{os.getenv('SNOWFLAKE_ACCOUNT')}.snowflakecomputing.com",
        "sfUser": os.getenv('SNOWFLAKE_USER'),
        "sfPassword": os.getenv('SNOWFLAKE_PASSWORD'),
        "sfDatabase": os.getenv('SNOWFLAKE_DATABASE'),
        "sfSchema": os.getenv('SNOWFLAKE_SCHEMA'),
        "sfWarehouse": os.getenv('SNOWFLAKE_WAREHOUSE'),
        "sfRole": os.getenv('SNOWFLAKE_ROLE')
    }

    try:
        print("Loading Fact Orders to Snowflake...")
        fact_orders_df.write \
            .format("snowflake") \
            .options(**sf_options) \
            .option("dbtable", f"{os.getenv('SNOWFLAKE_DATABASE')}.{os.getenv('SNOWFLAKE_SCHEMA')}.FACT_ORDERS") \
            .mode("overwrite") \
            .save()
            
        print("Loading Daily Sales Summary to Snowflake...")
        daily_sales_df.write \
            .format("snowflake") \
            .options(**sf_options) \
            .option("dbtable", f"{os.getenv('SNOWFLAKE_DATABASE')}.{os.getenv('SNOWFLAKE_SCHEMA')}.DAILY_SALES_SUMMARY") \
            .mode("overwrite") \
            .save()
            
        print("Successfully loaded data into Snowflake!")
        
    except Exception as e:
        print(f"Error loading to Snowflake: {e}")
        print("Please ensure your .env file is configured correctly and you have created the required Snowflake objects.")

if __name__ == "__main__":
    # We must include the snowflake-jdbc and spark-snowflake packages for this to work
    import sys
    spark = SparkSession.builder \
        .appName("ECommerce_ETL") \
        .config("spark.jars.packages", "net.snowflake:snowflake-jdbc:3.13.30,net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4") \
        .getOrCreate()
        
    RAW_DIR = "data/raw"
    
    c_df, p_df, o_df, r_df = extract_data(spark, RAW_DIR)
    fact_orders, daily_sales = transform_data(c_df, p_df, o_df, r_df)
    load_data(fact_orders, daily_sales)
    
    spark.stop()
