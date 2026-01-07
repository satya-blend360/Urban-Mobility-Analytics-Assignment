import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Set plotting style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)

class KPIAnalyzer:
    """
    KPI Computation and Visualization for Urban Mobility Data
    """
    
    def __init__(self, data):
        self.data = data
        self.kpis = {}
        
    def compute_all_kpis(self):
        """Compute all core KPIs"""
        print("\n" + "="*70)
        print("CORE KPI COMPUTATION")
        print("="*70)
        
        df = self.data
        
        # 1. Total and Monthly Revenue
        self.kpis['total_revenue'] = df['total_amount'].sum()
        self.kpis['monthly_revenue'] = df.groupby('month_name')['total_amount'].sum().to_dict()
        print(f"\n1. Revenue Metrics")
        print(f"   Total Revenue: ${self.kpis['total_revenue']:,.2f}")
        print(f"   Monthly Revenue:")
        for month, revenue in self.kpis['monthly_revenue'].items():
            print(f"     - {month}: ${revenue:,.2f}")
        
        # 2. Average Trip Distance
        self.kpis['avg_trip_distance'] = df['trip_distance'].mean()
        print(f"\n2. Trip Distance")
        print(f"   Average: {self.kpis['avg_trip_distance']:.2f} miles")
        print(f"   Median: {df['trip_distance'].median():.2f} miles")
        print(f"   Max: {df['trip_distance'].max():.2f} miles")
        
        # 3. Average Fare per Trip
        self.kpis['avg_fare'] = df['fare_amount'].mean()
        print(f"\n3. Fare Metrics")
        print(f"   Average Fare: ${self.kpis['avg_fare']:.2f}")
        print(f"   Median Fare: ${df['fare_amount'].median():.2f}")
        
        # 4. Tip Percentage
        self.kpis['avg_tip_percentage'] = df['tip_percentage'].mean()
        print(f"\n4. Tip Analysis")
        print(f"   Average Tip %: {self.kpis['avg_tip_percentage']:.2f}%")
        print(f"   Median Tip %: {df['tip_percentage'].median():.2f}%")
        
        # 5. Trips per Hour (Demand Pattern)
        self.kpis['trips_per_hour'] = df.groupby('hour_of_day').size().to_dict()
        busiest_hour = max(self.kpis['trips_per_hour'], key=self.kpis['trips_per_hour'].get)
        print(f"\n5. Demand Patterns")
        print(f"   Busiest Hour: {busiest_hour}:00 ({self.kpis['trips_per_hour'][busiest_hour]:,} trips)")
        print(f"   Total Trip Hours Analyzed: 24")
        
        # 6. Revenue per Mile
        self.kpis['avg_revenue_per_mile'] = df['revenue_per_mile'].mean()
        print(f"\n6. Revenue Efficiency")
        print(f"   Average Revenue per Mile: ${self.kpis['avg_revenue_per_mile']:.2f}")
        
        # 7. Peak vs Off-Peak Utilization
        peak_trips = df[df['is_peak_hour'] == 1]
        off_peak_trips = df[df['is_peak_hour'] == 0]
        
        self.kpis['peak_trips'] = len(peak_trips)
        self.kpis['off_peak_trips'] = len(off_peak_trips)
        self.kpis['peak_revenue'] = peak_trips['total_amount'].sum()
        self.kpis['off_peak_revenue'] = off_peak_trips['total_amount'].sum()
        self.kpis['peak_utilization_pct'] = (self.kpis['peak_trips'] / len(df)) * 100
        
        print(f"\n7. Peak vs Off-Peak Analysis")
        print(f"   Peak Hours (7-9 AM, 5-7 PM):")
        print(f"     - Trips: {self.kpis['peak_trips']:,} ({self.kpis['peak_utilization_pct']:.1f}%)")
        print(f"     - Revenue: ${self.kpis['peak_revenue']:,.2f}")
        print(f"     - Avg Fare: ${peak_trips['fare_amount'].mean():.2f}")
        print(f"   Off-Peak Hours:")
        print(f"     - Trips: {self.kpis['off_peak_trips']:,}")
        print(f"     - Revenue: ${self.kpis['off_peak_revenue']:,.2f}")
        print(f"     - Avg Fare: ${off_peak_trips['fare_amount'].mean():.2f}")
        
        # Additional KPIs
        print(f"\n8. Additional Insights")
        print(f"   Weekend Trips: {df['is_weekend'].sum():,} ({df['is_weekend'].mean()*100:.1f}%)")
        print(f"   Avg Trip Duration: {df['trip_duration_min'].mean():.2f} minutes")
        print(f"   Payment with Tips: {(df['tip_amount'] > 0).sum():,} ({(df['tip_amount'] > 0).mean()*100:.1f}%)")
        
        print("\n" + "="*70 + "\n")
        
        return self.kpis
    
    def visualize_monthly_revenue(self):
        """Visualize monthly revenue trends"""
        df = self.data
        monthly_data = df.groupby('month_name').agg({
            'total_amount': 'sum',
            'VendorID': 'count'
        }).reset_index()
        monthly_data.columns = ['Month', 'Revenue', 'Trips']
        
        # Sort by month order
        month_order = ['January', 'February', 'March', 'April', 'May', 'June',
                      'July', 'August', 'September', 'October', 'November', 'December']
        monthly_data['Month'] = pd.Categorical(monthly_data['Month'], categories=month_order, ordered=True)
        monthly_data = monthly_data.sort_values('Month')
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
        
        # Revenue trend
        ax1.plot(monthly_data['Month'], monthly_data['Revenue'], marker='o', 
                linewidth=2, markersize=8, color='#2E86AB')
        ax1.fill_between(range(len(monthly_data)), monthly_data['Revenue'], alpha=0.3, color='#2E86AB')
        ax1.set_title('Monthly Revenue Trends', fontsize=14, fontweight='bold')
        ax1.set_xlabel('Month', fontsize=12)
        ax1.set_ylabel('Revenue ($)', fontsize=12)
        ax1.grid(True, alpha=0.3)
        ax1.tick_params(axis='x', rotation=45)
        
        # Trip count
        ax2.bar(monthly_data['Month'], monthly_data['Trips'], color='#A23B72', alpha=0.7)
        ax2.set_title('Monthly Trip Volume', fontsize=14, fontweight='bold')
        ax2.set_xlabel('Month', fontsize=12)
        ax2.set_ylabel('Number of Trips', fontsize=12)
        ax2.grid(True, alpha=0.3, axis='y')
        ax2.tick_params(axis='x', rotation=45)
        
        plt.tight_layout()
        plt.savefig('monthly_revenue_trends.png', dpi=300, bbox_inches='tight')
        print("✓ Saved: monthly_revenue_trends.png")
        plt.show()
    
    def visualize_hourly_demand(self):
        """Create hourly demand heatmap"""
        df = self.data
        
        # Create pivot table for heatmap
        hourly_demand = df.groupby(['day_name', 'hour_of_day']).size().reset_index(name='trips')
        pivot_data = hourly_demand.pivot(index='day_name', columns='hour_of_day', values='trips')
        
        # Sort days
        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        pivot_data = pivot_data.reindex(day_order)
        
        plt.figure(figsize=(16, 6))
        sns.heatmap(pivot_data, annot=True, fmt='g', cmap='YlOrRd', cbar_kws={'label': 'Number of Trips'})
        plt.title('Hourly Demand Heatmap by Day of Week', fontsize=14, fontweight='bold', pad=20)
        plt.xlabel('Hour of Day', fontsize=12)
        plt.ylabel('Day of Week', fontsize=12)
        plt.tight_layout()
        plt.savefig('hourly_demand_heatmap.png', dpi=300, bbox_inches='tight')
        print("✓ Saved: hourly_demand_heatmap.png")
        plt.show()
    
    # def visualize_fare_distance_outliers(self):
    #     """Visualize fare and distance outliers"""
    #     df = self.data
        
    #     fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        
    #     # 1. Fare distribution
    #     axes[0, 0].hist(df['fare_amount'], bins=50, color='#2E86AB', alpha=0.7, edgecolor='black')
    #     axes[0, 0].axvline(df['fare_amount'].mean(), color='red', linestyle='--', linewidth=2, label=f'Mean: ${df["fare_amount"].mean():.2f}')
    #     axes[0, 0].axvline(df['fare_amount'].median(), color='green', linestyle='--', linewidth=2, label=f'Median: ${df["fare_amount"].median():.2f}')
    #     axes[0, 0].set_title('Fare Amount Distribution', fontsize=12, fontweight='bold')
    #     axes[0, 0].set_xlabel('Fare Amount ($)')
    #     axes[0, 0].set_ylabel('Frequency')
    #     axes[0, 0].legend()
    #     axes[0, 0].grid(True, alpha=0.3)
        
    #     # 2. Distance distribution
    #     axes[0, 1].hist(df['trip_distance'], bins=50, color='#A23B72', alpha=0.7, edgecolor='black')
    #     axes[0, 1].axvline(df['trip_distance'].mean(), color='red', linestyle='--', linewidth=2, label=f'Mean: {df["trip_distance"].mean():.2f} mi')
    #     axes[0, 1].axvline(df['trip_distance'].median(), color='green', linestyle='--', linewidth=2, label=f'Median: {df["trip_distance"].median():.2f} mi')
    #     axes[0, 1].set_title('Trip Distance Distribution', fontsize=12, fontweight='bold')
    #     axes[0, 1].set_xlabel('Trip Distance (miles)')
    #     axes[0, 1].set_ylabel('Frequency')
    #     axes[0, 1].legend()
    #     axes[0, 1].grid(True, alpha=0.3)
        
    #     # 3. Fare vs Distance scatter
    #     axes[1, 0].scatter(df['trip_distance'], df['fare_amount'], alpha=0.5, s=20, color='#F18F01')
    #     axes[1, 0].set_title('Fare vs Distance Relationship', fontsize=12, fontweight='bold')
    #     axes[1, 0].set_xlabel('Trip Distance (miles)')
    #     axes[1, 0].set_ylabel('Fare Amount ($)')
    #     axes[1, 0].grid(True, alpha=0.3)
        
    #     # 4. Box plots for outlier detection
    #     box_data = [df['fare_amount'], df['trip_distance'], df['tip_amount']]
    #     axes[1, 1].boxplot(box_data, labels=['Fare', 'Distance', 'Tip'])
    #     axes[1, 1].set_title('Outlier Detection (Box Plots)', fontsize=12, fontweight='bold')
    #     axes[1, 1].set_ylabel('Normalized Values')
    #     axes[1, 1].grid(True, alpha=0.3, axis='y')
        
    #     plt.tight_layout()
    #     plt.savefig('fare_distance_analysis.png', dpi=300, bbox_inches='tight')
    #     print("✓ Saved: fare_distance_analysis.png")
    #     plt.show()
    
    def visualize_tip_distribution(self):
        """Visualize tip distribution by time of day"""
        df = self.data
        
        fig, axes = plt.subplots(1, 3, figsize=(18, 6))
        
        # 1. Tip percentage by hour
        hourly_tips = df.groupby('hour_of_day')['tip_percentage'].mean()
        axes[0].bar(hourly_tips.index, hourly_tips.values, color='#06A77D', alpha=0.7)
        axes[0].set_title('Average Tip % by Hour of Day', fontsize=12, fontweight='bold')
        axes[0].set_xlabel('Hour of Day')
        axes[0].set_ylabel('Average Tip %')
        axes[0].grid(True, alpha=0.3, axis='y')
        
        # 2. Tip percentage by time category
        time_tips = df.groupby('time_of_day')['tip_percentage'].mean().sort_values()
        axes[1].barh(time_tips.index, time_tips.values, color=['#2E86AB', '#A23B72', '#F18F01', '#06A77D'])
        axes[1].set_title('Average Tip % by Time of Day', fontsize=12, fontweight='bold')
        axes[1].set_xlabel('Average Tip %')
        axes[1].grid(True, alpha=0.3, axis='x')
        
        # 3. Tip percentage by day of week
        day_tips = df.groupby('day_name')['tip_percentage'].mean()
        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        day_tips = day_tips.reindex(day_order)
        axes[2].plot(day_tips.index, day_tips.values, marker='o', linewidth=2, markersize=8, color='#D62246')
        axes[2].set_title('Average Tip % by Day of Week', fontsize=12, fontweight='bold')
        axes[2].set_xlabel('Day of Week')
        axes[2].set_ylabel('Average Tip %')
        axes[2].tick_params(axis='x', rotation=45)
        axes[2].grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig('tip_distribution_analysis.png', dpi=300, bbox_inches='tight')
        print("✓ Saved: tip_distribution_analysis.png")
        plt.show()
    
    def generate_all_visualizations(self):
        """Generate all visualization reports"""
        print("\n" + "="*70)
        print("GENERATING VISUALIZATIONS")
        print("="*70 + "\n")
        
        self.visualize_monthly_revenue()
        self.visualize_hourly_demand()
        # self.visualize_fare_distance_outliers()
        self.visualize_tip_distribution()
        
        print("\n" + "="*70)
        print("✓ All visualizations generated successfully!")
        print("="*70 + "\n")


# USAGE EXAMPLE
if __name__ == "__main__":
    # Load cleaned data
    df = pd.read_csv('cleaned_taxi_data.csv')
    
    # Convert datetime columns
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    
    print(f"Loaded {len(df):,} cleaned records for KPI analysis")
    
    # Initialize KPI Analyzer
    kpi_analyzer = KPIAnalyzer(df)
    
    # Compute all KPIs
    kpis = kpi_analyzer.compute_all_kpis()
    
    # Generate visualizations
    kpi_analyzer.generate_all_visualizations()
    
    print("\n✓ KPI analysis complete! Ready for SQL analytics.")