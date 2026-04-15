from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def write_to_pg(df, table_name):
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres_db_lab2:5432/lab2_db") \
        .option("dbtable", table_name) \
        .option("user", "admin") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

def main():
    spark = SparkSession.builder \
        .appName("Lab2_ETL_Pipeline") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")
    print("--- Spark Started ---")

    wide_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres_db_lab2:5432/lab2_db") \
        .option("dbtable", "mock_data") \
        .option("user", "admin") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .load()

    print("--- Star ---")
    
    dim_product = wide_df.select(
        F.col("sale_product_id").alias("product_id"), 
        "product_name", 
        "product_category", 
        "product_price"
    ).distinct()
    
    fact_sales = wide_df.select(
        F.col("id").alias("order_id"),
        F.col("sale_product_id").alias("product_id"),
        F.col("sale_customer_id").alias("customer_id"),
        F.col("sale_quantity").alias("quantity"),
        F.col("sale_total_price").alias("total_amount")
    )

    write_to_pg(dim_product, "dim_product")
    write_to_pg(fact_sales, "fact_sales")

    sales_enriched = fact_sales.join(dim_product, "product_id")

    top_10_products = sales_enriched \
        .groupBy("product_id", "product_name") \
        .agg(F.sum("quantity").alias("total_sold_quantity"),
             F.sum("total_amount").alias("total_revenue")) \
        .orderBy(F.desc("total_sold_quantity")) \
        .limit(10)

    print("--- Writing  to ClickHouse ---")
    top_10_products.printSchema()
    top_10_products.write \
        .format("jdbc") \
        .option("url", "jdbc:clickhouse:http://clickhouse:8123/reports_db") \
        .option("dbtable", "report_top_10_products") \
        .option("user", "admin") \
        .option("password", "password") \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .option("clickhouse.jdbc.jdbcCompliant", "false") \
        .option("compress", "false") \
        .option("decompress", "false") \
        .option("http_connection_provider", "HTTP_URL_CONNECTION") \
        .option("isolationLevel", "NONE") \
        .mode("append") \
        .save()

    print("--- Writing to MongoDB ---")
    top_10_products.write \
        .format("com.mongodb.spark.sql.DefaultSource") \
        .option("uri", "mongodb://admin:password@mongo_db:27017/reports_db?authSource=admin") \
        .option("database", "reports_db") \
        .option("collection", "report_top_10_products") \
        .mode("overwrite") \
        .save()

    print("--- Successfully Completed ---")
    spark.stop()

if __name__ == "__main__":
    main()