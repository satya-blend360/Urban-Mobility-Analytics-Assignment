import pandas as pd
import sqlite3
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

class SQLAnalyticsEngine:
    """
    SQL-based analytics engine for urban mobility data
    """
    
    def __init__(self, db_name='taxi_analytics.db'):
        self.db_name = db_name
        self.conn = None
        self.cursor = None
        
    def connect(self):
        """Create database connection"""
        try:
            self.conn = sqlite3.connect(self.db_name)
            self.cursor = self.conn.cursor()
            print(f"✓ Connected to database: {self.db_name}")
            return True
        except Exception as e:
            print(f"✗ Error connecting to database: {e}")
            return False
    
    def load_data_to_sql(self, csv_file='cleaned_taxi_data.csv'):
        """Load cleaned data into SQL database"""
        try:
            if self.conn is None or self.cursor is None:
                print("✗ Database connection not established. Call connect() first.")
                return False
                
            print(f"\nLoading data from {csv_file} into SQL database...")
            df = pd.read_csv(csv_file)
            
            # Load into SQL table
            df.to_sql('taxi_trips', self.conn, if_exists='replace', index=False)
            
            row_count = self.cursor.execute("SELECT COUNT(*) FROM taxi_trips").fetchone()[0]
            print(f"✓ Loaded {row_count:,} records into 'taxi_trips' table")
            
            # Show table schema
            schema = self.cursor.execute("PRAGMA table_info(taxi_trips)").fetchall()
            print(f"✓ Table has {len(schema)} columns")
            
            return True
        except Exception as e:
            print(f"✗ Error loading data: {e}")
            return False
    
    def execute_query(self, query_name, query, export=True):
        """Execute SQL query and optionally export results"""
        print("\n" + "="*70)
        print(f"QUERY: {query_name}")
        print("="*70)
        print(f"\nSQL:\n{query}\n")
        
        try:
            # Execute query
            result_df = pd.read_sql_query(query, self.conn)
            
            print(f"Results ({len(result_df)} rows):")
            print("-" * 70)
            print(result_df.to_string(index=False))
            print("-" * 70)
            
            # Export to CSV
            if export:
                filename = f"sql_result_{query_name.lower().replace(' ', '_')}.csv"
                result_df.to_csv(filename, index=False)
                print(f"\n✓ Results exported to: {filename}")
            
            return result_df
            
        except Exception as e:
            print(f"✗ Error executing query: {e}")
            return None
    
    def run_all_analytics(self):
        """Execute all analytical queries"""
        print("\n" + "="*70)
        print("SQL ANALYTICAL QUERIES")
        print("="*70)
        
        # Query 1: Peak Demand Hours
        query1 = """
        SELECT 
            hour_of_day,
            COUNT(*) as trip_count,
            ROUND(AVG(fare_amount), 2) as avg_fare,
            ROUND(SUM(total_amount), 2) as total_revenue,
            ROUND(AVG(trip_distance), 2) as avg_distance
        FROM taxi_trips
        GROUP BY hour_of_day
        ORDER BY trip_count DESC
        LIMIT 10
        """
        self.execute_query("Peak Demand Hours", query1)
        
        # Query 2: Revenue by Pickup Zone (using lat/long approximation)
        query2 = """
        SELECT 
            CASE 
                WHEN pickup_latitude BETWEEN 40.75 AND 40.78 THEN 'Midtown'
                WHEN pickup_latitude BETWEEN 40.70 AND 40.75 THEN 'Lower Manhattan'
                WHEN pickup_latitude BETWEEN 40.78 AND 40.82 THEN 'Upper Manhattan'
                ELSE 'Other'
            END as pickup_zone,
            COUNT(*) as trip_count,
            ROUND(SUM(total_amount), 2) as total_revenue,
            ROUND(AVG(total_amount), 2) as avg_revenue_per_trip
        FROM taxi_trips
        WHERE pickup_latitude IS NOT NULL
        GROUP BY pickup_zone
        ORDER BY total_revenue DESC
        """
        self.execute_query("Revenue by Pickup Zone", query2)
        
        # Query 3: Top 10 Highest Revenue Days
        query3 = """
        SELECT 
            date,
            day_name,
            COUNT(*) as trip_count,
            ROUND(SUM(total_amount), 2) as daily_revenue,
            ROUND(AVG(total_amount), 2) as avg_trip_revenue
        FROM taxi_trips
        GROUP BY date, day_name
        ORDER BY daily_revenue DESC
        LIMIT 10
        """
        self.execute_query("Top 10 Highest Revenue Days", query3)
        
        # Query 4: Average Fare by Weekday
        query4 = """
        SELECT 
            day_name,
            day_of_week,
            COUNT(*) as trip_count,
            ROUND(AVG(fare_amount), 2) as avg_fare,
            ROUND(AVG(total_amount), 2) as avg_total,
            ROUND(AVG(tip_percentage), 2) as avg_tip_pct
        FROM taxi_trips
        GROUP BY day_name, day_of_week
        ORDER BY day_of_week
        """
        self.execute_query("Average Fare by Weekday", query4)
        
        # Query 5: Monthly Growth Using Window Functions
        query5 = """
        SELECT 
            month,
            month_name,
            COUNT(*) as trip_count,
            ROUND(SUM(total_amount), 2) as monthly_revenue,
            ROUND(AVG(total_amount), 2) as avg_trip_value,
            ROUND(
                100.0 * (SUM(total_amount) - LAG(SUM(total_amount)) OVER (ORDER BY month)) 
                / LAG(SUM(total_amount)) OVER (ORDER BY month), 
                2
            ) as revenue_growth_pct
        FROM taxi_trips
        GROUP BY month, month_name
        ORDER BY month
        """
        self.execute_query("Monthly Growth Analysis", query5)
        
        # Query 6: Peak vs Off-Peak Performance
        query6 = """
        SELECT 
            CASE WHEN is_peak_hour = 1 THEN 'Peak Hours' ELSE 'Off-Peak Hours' END as period,
            COUNT(*) as trip_count,
            ROUND(SUM(total_amount), 2) as total_revenue,
            ROUND(AVG(total_amount), 2) as avg_fare,
            ROUND(AVG(trip_distance), 2) as avg_distance,
            ROUND(AVG(tip_percentage), 2) as avg_tip_pct
        FROM taxi_trips
        GROUP BY is_peak_hour
        """
        self.execute_query("Peak vs Off-Peak Performance", query6)
        
        # Query 7: Time of Day Analysis
        query7 = """
        SELECT 
            time_of_day,
            COUNT(*) as trip_count,
            ROUND(SUM(total_amount), 2) as revenue,
            ROUND(AVG(fare_amount), 2) as avg_fare,
            ROUND(AVG(trip_duration_min), 2) as avg_duration_min
        FROM taxi_trips
        GROUP BY time_of_day
        ORDER BY 
            CASE time_of_day
                WHEN 'Morning' THEN 1
                WHEN 'Afternoon' THEN 2
                WHEN 'Evening' THEN 3
                WHEN 'Night' THEN 4
            END
        """
        self.execute_query("Time of Day Performance", query7)
        
        # Query 8: High-Value Trips Analysis
        query8 = """
        SELECT 
            COUNT(*) as high_value_trips,
            ROUND(AVG(total_amount), 2) as avg_amount,
            ROUND(AVG(trip_distance), 2) as avg_distance,
            ROUND(AVG(trip_duration_min), 2) as avg_duration,
            ROUND(SUM(total_amount), 2) as total_revenue
        FROM taxi_trips
        WHERE total_amount > (SELECT AVG(total_amount) * 2 FROM taxi_trips)
        """
        self.execute_query("High-Value Trips (>2x Avg)", query8)
        
        # Query 9: Payment Type Analysis
        query9 = """
        SELECT 
            payment_type,
            COUNT(*) as trip_count,
            ROUND(AVG(total_amount), 2) as avg_amount,
            ROUND(AVG(tip_amount), 2) as avg_tip,
            ROUND(AVG(tip_percentage), 2) as avg_tip_pct
        FROM taxi_trips
        GROUP BY payment_type
        ORDER BY trip_count DESC
        """
        self.execute_query("Payment Type Analysis", query9)
        
        # Query 10: Weekend vs Weekday Comparison
        query10 = """
        SELECT 
            CASE WHEN is_weekend = 1 THEN 'Weekend' ELSE 'Weekday' END as period,
            COUNT(*) as trip_count,
            ROUND(SUM(total_amount), 2) as total_revenue,
            ROUND(AVG(total_amount), 2) as avg_fare,
            ROUND(AVG(trip_distance), 2) as avg_distance,
            ROUND(AVG(trip_duration_min), 2) as avg_duration
        FROM taxi_trips
        GROUP BY is_weekend
        """
        self.execute_query("Weekend vs Weekday Analysis", query10)
        
        print("\n" + "="*70)
        print("✓ All SQL analytics queries completed!")
        print("="*70)
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            print(f"\n✓ Database connection closed")


# USAGE EXAMPLE
if __name__ == "__main__":
    # Initialize SQL Analytics Engine
    sql_engine = SQLAnalyticsEngine('taxi_analytics.db')
    
    # Connect to database
    if sql_engine.connect():
        # Load data into SQL
        sql_engine.load_data_to_sql('cleaned_taxi_data.csv')
        
        # Run all analytical queries
        sql_engine.run_all_analytics()
        
        # Close connection
        sql_engine.close()
        
        print("\n✓ SQL analytics complete! Ready for PySpark ETL.")