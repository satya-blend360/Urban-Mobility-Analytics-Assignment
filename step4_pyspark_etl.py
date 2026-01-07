from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
import warnings
warnings.filterwarnings('ignore')

class PySparkETLPipeline:
    """
    Scalable ETL Pipeline for Urban Mobility Data using PySpark
    """
    
    def __init__(self, app_name="TaxiETL"):
        print("Initializing PySpark Session...")
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.driver.memory", "4g") \
            .config("spark.sql.shuffle.partitions", "8") \
            .getOrCreate()
        
        # Set log level to reduce verbosity
        self.spark.sparkContext.setLogLevel("ERROR")
        print(f"✓ Spark Session Created: {app_name}")
        print(f"  Spark Version: {self.spark.version}")
        
    def load_data(self, file_path):
        """Load CSV data into Spark DataFrame"""
        print(f"\nLoading data from: {file_path}")
        
        # Define schema for better performance
        schema = StructType([
            StructField("VendorID", IntegerType(), True),
            StructField("tpep_pickup_datetime", StringType(), True),
            StructField("tpep_dropoff_datetime", StringType(), True),
            StructField("passenger_count", DoubleType(), True),
            StructField("trip_distance", DoubleType(), True),
            StructField("pickup_longitude", DoubleType(), True),
            StructField("pickup_latitude", DoubleType(), True),
            StructField("RateCodeID", IntegerType(), True),
            StructField("store_and_fwd_flag", StringType(), True),
            StructField("dropoff_longitude", DoubleType(), True),
            StructField("dropoff_latitude", DoubleType(), True),
            StructField("payment_type", IntegerType(), True),
            StructField("fare_amount", DoubleType(), True),
            StructField("extra", DoubleType(), True),
            StructField("mta_tax", DoubleType(), True),
            StructField("tip_amount", DoubleType(), True),
            StructField("tolls_amount", DoubleType(), True),
            StructField("improvement_surcharge", DoubleType(), True),
            StructField("total_amount", DoubleType(), True)
        ])
        
        df = self.spark.read \
            .option("header", "true") \
            .schema(schema) \
            .csv(file_path)
        
        print(f"✓ Loaded {df.count():,} records")
        print(f"✓ Columns: {len(df.columns)}")
        
        return df
    
    def clean_and_transform(self, df):
        """Clean and transform data at scale"""
        print("\n" + "="*70)
        print("PYSPARK ETL: DATA CLEANING & TRANSFORMATION")
        print("="*70)
        
        initial_count = df.count()
        print(f"\n1. Initial Record Count: {initial_count:,}")
        
        # Convert timestamp columns
        df = df.withColumn("pickup_datetime", to_timestamp("tpep_pickup_datetime")) \
               .withColumn("dropoff_datetime", to_timestamp("tpep_dropoff_datetime"))
        
        # Data Quality Filters
        print("\n2. Applying Data Quality Filters")
        df = df.filter(col("trip_distance") > 0) \
               .filter(col("fare_amount") > 0) \
               .filter(col("total_amount") > 0) \
               .filter(col("dropoff_datetime") > col("pickup_datetime"))
        
        clean_count = df.count()
        removed = initial_count - clean_count
        print(f"   ✓ Removed {removed:,} invalid records ({removed/initial_count*100:.2f}%)")
        print(f"   ✓ Clean records: {clean_count:,}")
        
        # Feature Engineering
        print("\n3. Feature Engineering")
        
        # Time-based features
        df = df.withColumn("hour", hour("pickup_datetime")) \
               .withColumn("day_of_week", dayofweek("pickup_datetime")) \
               .withColumn("month", month("pickup_datetime")) \
               .withColumn("year", year("pickup_datetime")) \
               .withColumn("date", to_date("pickup_datetime"))
        
        print("   ✓ Time-based features: hour, day_of_week, month, year, date")
        
        # Trip duration
        df = df.withColumn("trip_duration_min", 
                          (unix_timestamp("dropoff_datetime") - unix_timestamp("pickup_datetime")) / 60)
        
        # Tip percentage
        df = df.withColumn("tip_percentage", 
                          round((col("tip_amount") / col("fare_amount")) * 100, 2))
        
        # Revenue per mile
        df = df.withColumn("revenue_per_mile", 
                          round(col("total_amount") / col("trip_distance"), 2))
        
        # Peak hour indicator
        df = df.withColumn("is_peak_hour", 
                          when(col("hour").isin([7,8,9,17,18,19]), 1).otherwise(0))
        
        # Weekend indicator
        df = df.withColumn("is_weekend", 
                          when(col("day_of_week").isin([1,7]), 1).otherwise(0))
        
        # Zone classification (simplified)
        df = df.withColumn("pickup_zone",
                          when((col("pickup_latitude") >= 40.75) & (col("pickup_latitude") < 40.78), "Midtown")
                          .when((col("pickup_latitude") >= 40.70) & (col("pickup_latitude") < 40.75), "Lower Manhattan")
                          .when((col("pickup_latitude") >= 40.78) & (col("pickup_latitude") < 40.82), "Upper Manhattan")
                          .otherwise("Other"))
        
        print("   ✓ Analytical features: trip_duration_min, tip_percentage, revenue_per_mile")
        print("   ✓ Indicator features: is_peak_hour, is_weekend")
        print("   ✓ Zone classification: pickup_zone")
        
        print(f"\n4. Final Transformed Dataset")
        print(f"   Records: {df.count():,}")
        print(f"   Columns: {len(df.columns)}")
        
        return df
    
    def compute_kpis(self, df):
        """Compute KPIs at scale"""
        print("\n" + "="*70)
        print("PYSPARK: KPI COMPUTATION")
        print("="*70)
        
        # Register temp view for SQL queries
        df.createOrReplaceTempView("taxi_trips")
        
        # KPI 1: Monthly Revenue
        print("\n1. Monthly Revenue")
        monthly_revenue = df.groupBy("year", "month") \
            .agg(
                count("*").alias("trip_count"),
                round(sum("total_amount"), 2).alias("total_revenue"),
                round(avg("total_amount"), 2).alias("avg_revenue_per_trip")
            ) \
            .orderBy("year", "month")
        
        monthly_revenue.show()
        monthly_revenue.write.mode("overwrite").parquet("output/monthly_revenue.parquet")
        print("   ✓ Saved to: output/monthly_revenue.parquet")
        
        # KPI 2: Demand by Zone
        print("\n2. Demand by Pickup Zone")
        zone_demand = df.groupBy("pickup_zone") \
            .agg(
                count("*").alias("trip_count"),
                round(sum("total_amount"), 2).alias("total_revenue"),
                round(avg("trip_distance"), 2).alias("avg_distance")
            ) \
            .orderBy(desc("trip_count"))
        
        zone_demand.show()
        zone_demand.write.mode("overwrite").parquet("output/zone_demand.parquet")
        print("   ✓ Saved to: output/zone_demand.parquet")
        
        # KPI 3: Peak Hour Congestion
        print("\n3. Peak Hour Analysis")
        peak_analysis = df.groupBy("hour", "is_peak_hour") \
            .agg(
                count("*").alias("trip_count"),
                round(avg("trip_duration_min"), 2).alias("avg_duration"),
                round(avg("total_amount"), 2).alias("avg_fare")
            ) \
            .orderBy("hour")
        
        peak_analysis.show(24)
        peak_analysis.write.mode("overwrite").parquet("output/peak_hour_analysis.parquet")
        print("   ✓ Saved to: output/peak_hour_analysis.parquet")
        
        # KPI 4: High-Value Trip Segments
        print("\n4. High-Value Trip Segments")
        
        # Calculate percentile threshold
        avg_fare = df.agg(avg("total_amount")).collect()[0][0]
        high_value_threshold = avg_fare * 2
        
        high_value_trips = df.filter(col("total_amount") > high_value_threshold) \
            .groupBy("hour", "pickup_zone") \
            .agg(
                count("*").alias("high_value_trip_count"),
                round(avg("total_amount"), 2).alias("avg_high_value_fare"),
                round(sum("total_amount"), 2).alias("high_value_revenue")
            ) \
            .orderBy(desc("high_value_revenue"))
        
        print(f"   High-value threshold: ${high_value_threshold:.2f}")
        high_value_trips.show(10)
        high_value_trips.write.mode("overwrite").parquet("output/high_value_segments.parquet")
        print("   ✓ Saved to: output/high_value_segments.parquet")
        
        # KPI 5: Day-of-Week Performance
        print("\n5. Day-of-Week Performance")
        dow_performance = df.groupBy("day_of_week", "is_weekend") \
            .agg(
                count("*").alias("trip_count"),
                round(sum("total_amount"), 2).alias("revenue"),
                round(avg("total_amount"), 2).alias("avg_fare"),
                round(avg("tip_percentage"), 2).alias("avg_tip_pct")
            ) \
            .orderBy("day_of_week")
        
        dow_performance.show()
        dow_performance.write.mode("overwrite").parquet("output/dow_performance.parquet")
        print("   ✓ Saved to: output/dow_performance.parquet")
        
        print("\n" + "="*70)
        print("✓ All KPIs computed and saved to Parquet format")
        print("="*70)
    
    def show_execution_plan(self, df):
        """Display Spark execution plan"""
        print("\n" + "="*70)
        print("SPARK EXECUTION PLAN (DAG)")
        print("="*70)
        
        # Show logical plan
        print("\nLogical Plan:")
        print("-" * 70)
        df.explain(extended=False)
        
        # Show physical plan
        print("\nPhysical Plan:")
        print("-" * 70)
        df.explain(mode="formatted")
        
        print("\n" + "="*70)
    
    def performance_benefits(self):
        """Explain PySpark performance benefits"""
        print("\n" + "="*70)
        print("PYSPARK PERFORMANCE BENEFITS")
        print("="*70)
        
        benefits = """
        1. DISTRIBUTED PROCESSING
           - Data is partitioned across multiple nodes
           - Parallel processing of partitions
           - Horizontal scalability (add more nodes for larger datasets)
        
        2. IN-MEMORY COMPUTATION
           - Data cached in RAM for faster access
           - Reduces disk I/O operations
           - 10-100x faster than traditional MapReduce
        
        3. LAZY EVALUATION
           - Operations are not executed until an action is called
           - Spark optimizes the entire execution plan
           - Reduces unnecessary computations
        
        4. CATALYST OPTIMIZER
           - Automatic query optimization
           - Predicate pushdown (filter early)
           - Column pruning (only read needed columns)
        
        5. FAULT TOLERANCE
           - RDD lineage for recovery
           - Automatic retry of failed tasks
           - No data loss on node failure
        
        6. COLUMNAR STORAGE (Parquet)
           - Efficient compression
           - Column-wise data access
           - Faster for analytical queries
        
        7. SCALABILITY TO 100GB+
           - Process petabyte-scale data
           - Add nodes linearly for more capacity
           - No code changes needed for scaling
        """
        
        print(benefits)
        print("="*70)
    
    def stop(self):
        """Stop Spark session"""
        self.spark.stop()
        print("\n✓ Spark session stopped")


# USAGE EXAMPLE
if __name__ == "__main__":
    # Initialize ETL Pipeline
    etl = PySparkETLPipeline(app_name="NYC_Taxi_ETL")
    
    try:
        # Load data
        df = etl.load_data("yellow_tripdata.csv")
        
        # Clean and transform
        clean_df = etl.clean_and_transform(df)
        
        # Compute KPIs
        etl.compute_kpis(clean_df)
        
        # Show execution plan (on a sample aggregation)
        sample_agg = clean_df.groupBy("hour").agg(count("*").alias("trips"))
        etl.show_execution_plan(sample_agg)
        
        # Explain performance benefits
        etl.performance_benefits()
        
        print("\n✓ PySpark ETL pipeline complete! Ready for GenAI insights.")
        
    finally:
        # Clean up
        etl.stop()