import pandas as pd
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

class MobilityDataAnalyzer:
    """
    Urban Mobility Data Analyzer for NYC Taxi Trip Data
    Handles data ingestion, cleaning, and feature engineering
    """
    
    def __init__(self):
        self.raw_data = None
        self.cleaned_data = None
        self.data_quality_report = {}
        
    def load_data(self, file_path):
        """Load raw taxi trip data from CSV"""
        try:
            print(f"Loading data from {file_path}...")
            self.raw_data = pd.read_csv(file_path)
            print(f"✓ Loaded {len(self.raw_data)} records")
            print(f"✓ Columns: {list(self.raw_data.columns)}")
            return self.raw_data
        except Exception as e:
            print(f"✗ Error loading data: {e}")
            return None
    
    def clean_data(self):
        """Clean and validate trip data"""
        if self.raw_data is None:
            print("✗ No data loaded. Call load_data() first.")
            return None
        
        print("\n" + "="*60)
        print("DATA CLEANING PROCESS")
        print("="*60)
        
        df = self.raw_data.copy()
        initial_count = len(df)
        
        # Track data quality issues
        self.data_quality_report: dict = {
            'initial_records': initial_count,
            'missing_passenger_count': df['passenger_count'].isna().sum(),
            'zero_distance': (df['trip_distance'] <= 0).sum(),
            'negative_fare': (df['fare_amount'] <= 0).sum(),
            'invalid_timestamps': 0
        }
        
        print(f"\n1. Initial Record Count: {initial_count}")
        
        # Handle missing passenger counts (impute with median)
        if df['passenger_count'].isna().sum() > 0:
            median_passengers = df['passenger_count'].median()
            df['passenger_count'] = df['passenger_count'].fillna(int(median_passengers))
            print(f"   ✓ Filled {self.data_quality_report['missing_passenger_count']} missing passenger counts with median: {int(median_passengers)}")
        
        # Remove invalid trip distances
        print(f"\n2. Removing Invalid Trip Distances")
        print(f"   - Zero or negative distances: {self.data_quality_report['zero_distance']}")
        df = df[df['trip_distance'] > 0]
        
        # Remove negative fares
        print(f"\n3. Removing Invalid Fares")
        print(f"   - Negative fares: {self.data_quality_report['negative_fare']}")
        df = df[df['fare_amount'] > 0]
        df = df[df['total_amount'] > 0]
        
        # Convert timestamps to datetime
        print(f"\n4. Converting Timestamps")
        try:
            df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
            df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
            print(f"   ✓ Converted pickup and dropoff timestamps to datetime")
            
            # Remove invalid timestamps (dropoff before pickup)
            invalid_time = df['tpep_dropoff_datetime'] <= df['tpep_pickup_datetime']
            self.data_quality_report['invalid_timestamps'] = invalid_time.sum()
            df = df[~invalid_time]
            print(f"   ✓ Removed {self.data_quality_report['invalid_timestamps']} records with invalid timestamps")
        except Exception as e:
            print(f"   ✗ Error converting timestamps: {e}")
        
        # Convert numeric columns
        print(f"\n5. Converting Numeric Columns")
        numeric_cols = ['fare_amount', 'tip_amount', 'total_amount', 'trip_distance', 'extra', 'tolls_amount']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        print(f"   ✓ Converted {len(numeric_cols)} numeric columns")
        
        # Remove any remaining NaN values
        df = df.dropna()
        
        final_count = len(df)
        self.data_quality_report['final_records'] = final_count
        self.data_quality_report['records_removed'] = initial_count - final_count
        self.data_quality_report['removal_percentage'] = (initial_count - final_count) / initial_count * 100
        
        print(f"\n" + "="*60)
        print(f"CLEANING SUMMARY")
        print(f"="*60)
        print(f"Initial Records:  {initial_count:,}")
        print(f"Final Records:    {final_count:,}")
        print(f"Records Removed:  {initial_count - final_count:,} ({self.data_quality_report['removal_percentage']:.2f}%)")
        print(f"="*60 + "\n")
        
        self.cleaned_data = df
        return self.cleaned_data
    
    def feature_engineering(self):
        """Create time-based and analytical features"""
        if self.cleaned_data is None:
            print("✗ No clean data available. Call clean_data() first.")
            return None
        
        print("\n" + "="*60)
        print("FEATURE ENGINEERING")
        print("="*60)
        
        df = self.cleaned_data.copy()
        
        # Time-based features
        print("\n1. Creating Time-Based Features")
        
        # Store pickup_datetime as datetime before extracting features
        pickup_datetime = pd.to_datetime(df['tpep_pickup_datetime'])
        
        df['hour_of_day'] = pickup_datetime.dt.hour
        df['day_of_week'] = pickup_datetime.dt.dayofweek  # 0=Monday, 6=Sunday
        df['day_name'] = pickup_datetime.dt.day_name()
        df['month'] = pickup_datetime.dt.month
        df['month_name'] = pickup_datetime.dt.month_name()
        df['quarter'] = ((pickup_datetime.dt.month - 1) // 3) + 1
        df['year'] = pickup_datetime.dt.year
        df['date'] = pickup_datetime.dt.date
        
        print(f"   ✓ hour_of_day (0-23)")
        print(f"   ✓ day_of_week (0=Monday, 6=Sunday)")
        print(f"   ✓ day_name")
        print(f"   ✓ month (1-12)")
        print(f"   ✓ month_name")
        print(f"   ✓ quarter (1-4)")
        print(f"   ✓ year")
        print(f"   ✓ date")
        
        # Analytical features
        print("\n2. Creating Analytical Features")
        
        # Trip duration in minutes
        df['trip_duration_min'] = (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']).dt.total_seconds() / 60 # type: ignore
        print(f"   ✓ trip_duration_min")
        
        # Tip percentage
        df['tip_percentage'] = (df['tip_amount'] / df['fare_amount'] * 100).round(2)
        df['tip_percentage'] = df['tip_percentage'].clip(0, 100)  # Cap at 100%
        print(f"   ✓ tip_percentage")
        
        # Revenue per mile
        df['revenue_per_mile'] = (df['total_amount'] / df['trip_distance']).round(2)
        print(f"   ✓ revenue_per_mile")
        
        # Peak hours indicator (7-9 AM and 5-7 PM)
        df['is_peak_hour'] = df['hour_of_day'].isin([7, 8, 9, 17, 18, 19]).astype(int)
        print(f"   ✓ is_peak_hour (1=peak, 0=off-peak)")
        
        # Weekend indicator
        df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
        print(f"   ✓ is_weekend (1=weekend, 0=weekday)")
        
        # Time of day category
        def categorize_time(hour):
            if 6 <= hour < 12:
                return 'Morning'
            elif 12 <= hour < 17:
                return 'Afternoon'
            elif 17 <= hour < 21:
                return 'Evening'
            else:
                return 'Night'
        
        df['time_of_day'] = df['hour_of_day'].apply(categorize_time)
        print(f"   ✓ time_of_day (Morning/Afternoon/Evening/Night)")
        
        print(f"\n" + "="*60)
        print(f"Total Features Created: 15")
        print(f"Total Columns in Dataset: {len(df.columns)}")
        print(f"="*60 + "\n")
        
        self.cleaned_data = df
        return self.cleaned_data
    
    def export_clean_data(self, output_path='cleaned_taxi_data.csv'):
        """Export cleaned data to CSV"""
        if self.cleaned_data is None:
            print("✗ No clean data available. Complete cleaning and feature engineering first.")
            return False
        
        try:
            self.cleaned_data.to_csv(output_path, index=False)
            print(f"\n✓ Clean data exported to: {output_path}")
            print(f"  Records: {len(self.cleaned_data):,}")
            print(f"  Columns: {len(self.cleaned_data.columns)}")
            return True
        except Exception as e:
            print(f"✗ Error exporting data: {e}")
            return False
    
    def get_summary_statistics(self):
        """Display summary statistics of cleaned data"""
        if self.cleaned_data is None:
            print("✗ No clean data available.")
            return None
        
        print("\n" + "="*60)
        print("SUMMARY STATISTICS")
        print("="*60)
        
        df = self.cleaned_data
        
        print(f"\nTrip Statistics:")
        print(f"  Total Trips: {len(df):,}")
        print(f"  Avg Trip Distance: {df['trip_distance'].mean():.2f} miles")
        print(f"  Avg Trip Duration: {df['trip_duration_min'].mean():.2f} minutes")
        print(f"  Avg Fare: ${df['fare_amount'].mean():.2f}")
        print(f"  Avg Total Amount: ${df['total_amount'].mean():.2f}")
        print(f"  Avg Tip: ${df['tip_amount'].mean():.2f}")
        print(f"  Avg Tip %: {df['tip_percentage'].mean():.2f}%")
        
        print(f"\nRevenue Statistics:")
        print(f"  Total Revenue: ${df['total_amount'].sum():,.2f}")
        print(f"  Avg Revenue per Mile: ${df['revenue_per_mile'].mean():.2f}")
        
        print(f"\nTime Patterns:")
        print(f"  Peak Hour Trips: {df['is_peak_hour'].sum():,} ({df['is_peak_hour'].mean()*100:.1f}%)")
        print(f"  Weekend Trips: {df['is_weekend'].sum():,} ({df['is_weekend'].mean()*100:.1f}%)")
        
        print("\n" + "="*60 + "\n")


# USAGE EXAMPLE
if __name__ == "__main__":
    # Initialize analyzer
    analyzer = MobilityDataAnalyzer()
    
    # Step 1: Load data
    analyzer.load_data('yellow_tripdata.csv')
    
    # Step 2: Clean data
    analyzer.clean_data()
    
    # Step 3: Feature engineering
    analyzer.feature_engineering()
    
    # Step 4: Export clean data
    analyzer.export_clean_data('cleaned_taxi_data.csv')
    
    # Step 5: Get summary statistics
    analyzer.get_summary_statistics()
    
    print("\n✓ Data preparation complete! Ready for KPI analysis.")