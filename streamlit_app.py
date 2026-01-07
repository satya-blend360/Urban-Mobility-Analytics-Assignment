import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import sqlite3
from io import StringIO

# Page configuration
st.set_page_config(
    page_title="Urban Mobility Analytics Platform",
    page_icon="🚖",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1.5rem;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .insight-box {
        background-color: #e8f4f8;
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #1f77b4;
        margin: 1rem 0;
    }
    </style>
""", unsafe_allow_html=True)

# Helper Functions
@st.cache_data
def load_data():
    """Load and cache the taxi data (first 10,000 rows for performance)"""
    df = pd.read_csv('cleaned_taxi_data.csv', nrows=10000)
    
    # Convert datetime columns
    datetime_cols = ['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'date']
    for col in datetime_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])
    
    return df

# Load data once at startup
try:
    data = load_data()
    data_loaded = True
except FileNotFoundError:
    data_loaded = False
    data = None

def calculate_kpis(df):
    """Calculate key performance indicators"""
    kpis = {
        'total_trips': len(df),
        'total_revenue': df['total_amount'].sum(),
        'avg_fare': df['fare_amount'].mean(),
        'avg_distance': df['trip_distance'].mean(),
        'avg_tip_percentage': df['tip_percentage'].mean(),
        'total_distance': df['trip_distance'].sum(),
        'revenue_per_mile': df['revenue_per_mile'].mean(),
        'avg_duration': df['trip_duration_min'].mean()
    }
    return kpis

def create_sql_connection(df):
    """Create SQLite database from dataframe"""
    conn = sqlite3.connect(':memory:')
    df.to_sql('trips', conn, index=False, if_exists='replace')
    return conn

# Sidebar
st.sidebar.markdown("## 🚖 Navigation")
page = st.sidebar.radio(
    "Select Page",
    ["📊 Dashboard", "📈 Analytics", "🔍 SQL Insights", "🤖 GenAI Assistant", "📁 Data Explorer"]
)

st.sidebar.markdown("---")
if data_loaded:
    st.sidebar.success(f"✅ Data loaded")
else:
    st.sidebar.error("❌ Error: cleaned_taxi_data.csv not found in the current directory")

# Main Content
st.markdown("<h1 class='main-header'>🚖 Urban Mobility Analytics Platform</h1>", unsafe_allow_html=True)

if data_loaded:
    df = data
    
    # ============================================
    # PAGE 1: DASHBOARD
    # ============================================
    if page == "📊 Dashboard":
        st.header("Executive Dashboard")
        
        # Date range filter
        col1, col2 = st.columns(2)
        with col1:
            date_range = st.date_input(
                "Select Date Range",
                value=(df['date'].min(), df['date'].max()),
                min_value=df['date'].min(),
                max_value=df['date'].max()
            )
        
        # Filter data by date range
        if len(date_range) == 2:
            mask = (df['date'].dt.date >= date_range[0]) & (df['date'].dt.date <= date_range[1])
            filtered_df = df[mask]
        else:
            filtered_df = df
        
        # Calculate KPIs
        kpis = calculate_kpis(filtered_df)
        
        # Display KPIs
        st.subheader("Key Performance Indicators")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "Total Trips",
                f"{kpis['total_trips']:,}",
                help="Total number of trips in selected period"
            )
            st.metric(
                "Avg Trip Distance",
                f"{kpis['avg_distance']:.2f} mi",
                help="Average distance per trip"
            )
        
        with col2:
            st.metric(
                "Total Revenue",
                f"${kpis['total_revenue']:,.2f}",
                help="Total revenue generated"
            )
            st.metric(
                "Revenue per Mile",
                f"${kpis['revenue_per_mile']:.2f}",
                help="Average revenue earned per mile"
            )
        
        with col3:
            st.metric(
                "Average Fare",
                f"${kpis['avg_fare']:.2f}",
                help="Average fare amount per trip"
            )
            st.metric(
                "Avg Tip %",
                f"{kpis['avg_tip_percentage']:.1f}%",
                help="Average tip percentage"
            )
        
        with col4:
            st.metric(
                "Total Distance",
                f"{kpis['total_distance']:,.0f} mi",
                help="Total miles traveled"
            )
            st.metric(
                "Avg Duration",
                f"{kpis['avg_duration']:.1f} min",
                help="Average trip duration"
            )
        
        st.markdown("---")
        
        # Visualizations
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Daily Revenue Trend")
            daily_revenue = filtered_df.groupby('date')['total_amount'].sum().reset_index()
            fig = px.line(
                daily_revenue,
                x='date',
                y='total_amount',
                title='Daily Revenue Over Time',
                labels={'total_amount': 'Revenue ($)', 'date': 'Date'}
            )
            fig.update_traces(line_color='#1f77b4', line_width=2)
            fig.update_layout(hovermode='x unified')
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("Hourly Demand Pattern")
            hourly_trips = filtered_df.groupby('hour_of_day').size().reset_index(name='trips')
            fig = px.bar(
                hourly_trips,
                x='hour_of_day',
                y='trips',
                title='Trip Distribution by Hour',
                labels={'hour_of_day': 'Hour of Day', 'trips': 'Number of Trips'},
                color='trips',
                color_continuous_scale='Blues'
            )
            st.plotly_chart(fig, use_container_width=True)
        
        col3, col4 = st.columns(2)
        
        with col3:
            st.subheader("Trips by Day of Week")
            dow_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            dow_trips = filtered_df.groupby('day_name').size().reset_index(name='trips')
            dow_trips['day_name'] = pd.Categorical(dow_trips['day_name'], categories=dow_order, ordered=True)
            dow_trips = dow_trips.sort_values('day_name')
            
            fig = px.bar(
                dow_trips,
                x='day_name',
                y='trips',
                title='Trip Volume by Day of Week',
                labels={'day_name': 'Day', 'trips': 'Number of Trips'},
                color='trips',
                color_continuous_scale='Viridis'
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col4:
            st.subheader("Payment Type Distribution")
            payment_dist = filtered_df['payment_type'].value_counts().reset_index()
            payment_dist.columns = ['payment_type', 'count']
            payment_map = {1: 'Credit Card', 2: 'Cash', 3: 'No Charge', 4: 'Dispute'}
            payment_dist['payment_type'] = payment_dist['payment_type'].map(payment_map)
            
            fig = px.pie(
                payment_dist,
                values='count',
                names='payment_type',
                title='Payment Methods Used',
                hole=0.4
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Peak vs Off-Peak Analysis
        st.subheader("Peak vs Off-Peak Analysis")
        col1, col2 = st.columns(2)
        
        with col1:
            peak_stats = filtered_df.groupby('is_peak_hour').agg({
                'total_amount': ['sum', 'mean'],
                'trip_distance': 'mean',
                'tip_percentage': 'mean'
            }).round(2)
            peak_stats.index = ['Off-Peak', 'Peak']
            st.dataframe(peak_stats, use_container_width=True)
        
        with col2:
            peak_revenue = filtered_df.groupby('is_peak_hour')['total_amount'].sum().reset_index()
            peak_revenue['is_peak_hour'] = peak_revenue['is_peak_hour'].map({0: 'Off-Peak', 1: 'Peak'})
            
            fig = px.bar(
                peak_revenue,
                x='is_peak_hour',
                y='total_amount',
                title='Revenue: Peak vs Off-Peak Hours',
                labels={'is_peak_hour': 'Time Period', 'total_amount': 'Revenue ($)'},
                color='is_peak_hour',
                color_discrete_map={'Peak': '#ff7f0e', 'Off-Peak': '#2ca02c'}
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # ============================================
    # PAGE 2: ANALYTICS
    # ============================================
    elif page == "📈 Analytics":
        st.header("Advanced Analytics")
        
        tab1, tab2, tab3, tab4 = st.tabs(["📊 Revenue Analysis", "🗺️ Spatial Analysis", "⏰ Time Analysis", "💡 Insights"])
        
        with tab1:
            st.subheader("Revenue Deep Dive")
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Monthly revenue trend
                st.markdown("#### Monthly Revenue Trend")
                monthly_revenue = df.groupby('month_name')['total_amount'].sum().reset_index()
                month_order = ['January', 'February', 'March', 'April', 'May', 'June', 
                              'July', 'August', 'September', 'October', 'November', 'December']
                monthly_revenue['month_name'] = pd.Categorical(
                    monthly_revenue['month_name'], 
                    categories=month_order, 
                    ordered=True
                )
                monthly_revenue = monthly_revenue.sort_values('month_name')
                
                fig = px.bar(
                    monthly_revenue,
                    x='month_name',
                    y='total_amount',
                    title='Monthly Revenue Distribution',
                    labels={'month_name': 'Month', 'total_amount': 'Revenue ($)'},
                    color='total_amount',
                    color_continuous_scale='Blues'
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Revenue distribution
                st.markdown("#### Fare Distribution")
                fig = px.histogram(
                    df[df['fare_amount'] < 100],
                    x='fare_amount',
                    nbins=50,
                    title='Fare Amount Distribution (< $100)',
                    labels={'fare_amount': 'Fare Amount ($)'},
                    color_discrete_sequence=['#1f77b4']
                )
                st.plotly_chart(fig, use_container_width=True)
            
            # Tip analysis
            st.markdown("#### Tip Analysis by Time of Day")
            tip_by_time = df.groupby('time_of_day')['tip_percentage'].mean().reset_index()
            time_order = ['Morning', 'Afternoon', 'Evening', 'Night']
            tip_by_time['time_of_day'] = pd.Categorical(
                tip_by_time['time_of_day'],
                categories=time_order,
                ordered=True
            )
            tip_by_time = tip_by_time.sort_values('time_of_day')
            
            fig = px.bar(
                tip_by_time,
                x='time_of_day',
                y='tip_percentage',
                title='Average Tip Percentage by Time of Day',
                labels={'time_of_day': 'Time of Day', 'tip_percentage': 'Avg Tip (%)'},
                color='tip_percentage',
                color_continuous_scale='Greens'
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with tab2:
            st.subheader("Spatial Analysis")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("#### Pickup Location Heatmap")
                # Sample data for performance
                sample_df = df.sample(min(5000, len(df)))
                
                fig = px.density_mapbox(
                    sample_df,
                    lat='pickup_latitude',
                    lon='pickup_longitude',
                    radius=10,
                    zoom=10,
                    mapbox_style="open-street-map",
                    title='Pickup Density Map',
                    height=500
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                st.markdown("#### Trip Distance Distribution")
                fig = px.histogram(
                    df[df['trip_distance'] < 20],
                    x='trip_distance',
                    nbins=50,
                    title='Trip Distance Distribution (< 20 miles)',
                    labels={'trip_distance': 'Distance (miles)'},
                    color_discrete_sequence=['#2ca02c']
                )
                st.plotly_chart(fig, use_container_width=True)
        
        with tab3:
            st.subheader("Temporal Patterns")
            
            # Heatmap: Hour vs Day of Week
            st.markdown("#### Demand Heatmap: Hour vs Day of Week")
            heatmap_data = df.groupby(['day_name', 'hour_of_day']).size().reset_index(name='trips')
            heatmap_pivot = heatmap_data.pivot(index='day_name', columns='hour_of_day', values='trips')
            
            dow_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            heatmap_pivot = heatmap_pivot.reindex(dow_order)
            
            fig = px.imshow(
                heatmap_pivot,
                labels=dict(x="Hour of Day", y="Day of Week", color="Trips"),
                x=heatmap_pivot.columns,
                y=heatmap_pivot.index,
                color_continuous_scale='YlOrRd',
                title='Trip Demand Heatmap'
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Weekend vs Weekday
            col1, col2 = st.columns(2)
            
            with col1:
                weekend_stats = df.groupby('is_weekend').agg({
                    'total_amount': 'sum',
                    'trip_distance': 'mean',
                    'tip_percentage': 'mean'
                }).round(2)
                weekend_stats.index = ['Weekday', 'Weekend']
                st.markdown("#### Weekend vs Weekday Stats")
                st.dataframe(weekend_stats, use_container_width=True)
            
            with col2:
                weekend_trips = df.groupby('is_weekend').size().reset_index(name='trips')
                weekend_trips['is_weekend'] = weekend_trips['is_weekend'].map({0: 'Weekday', 1: 'Weekend'})
                
                fig = px.pie(
                    weekend_trips,
                    values='trips',
                    names='is_weekend',
                    title='Weekday vs Weekend Trip Distribution',
                    hole=0.4,
                    color_discrete_sequence=['#1f77b4', '#ff7f0e']
                )
                st.plotly_chart(fig, use_container_width=True)
        
        with tab4:
            st.subheader("Key Insights")
            
            # Generate insights
            kpis = calculate_kpis(df)
            
            st.markdown("<div class='insight-box'>", unsafe_allow_html=True)
            st.markdown("### 📊 Revenue Insights")
            st.write(f"- Total revenue generated: **${kpis['total_revenue']:,.2f}**")
            st.write(f"- Average revenue per mile: **${kpis['revenue_per_mile']:.2f}**")
            
            peak_revenue = df[df['is_peak_hour'] == 1]['total_amount'].sum()
            peak_pct = (peak_revenue / kpis['total_revenue']) * 100
            st.write(f"- Peak hours contribute **{peak_pct:.1f}%** of total revenue")
            st.markdown("</div>", unsafe_allow_html=True)
            
            st.markdown("<div class='insight-box'>", unsafe_allow_html=True)
            st.markdown("### 🚖 Trip Patterns")
            busiest_hour = df.groupby('hour_of_day').size().idxmax()
            busiest_day = df.groupby('day_name').size().idxmax()
            st.write(f"- Busiest hour: **{busiest_hour}:00**")
            st.write(f"- Busiest day: **{busiest_day}**")
            st.write(f"- Average trip duration: **{kpis['avg_duration']:.1f} minutes**")
            st.markdown("</div>", unsafe_allow_html=True)
            
            st.markdown("<div class='insight-box'>", unsafe_allow_html=True)
            st.markdown("### 💰 Customer Behavior")
            st.write(f"- Average tip percentage: **{kpis['avg_tip_percentage']:.1f}%**")
            credit_pct = (df[df['payment_type'] == 1].shape[0] / len(df)) * 100
            st.write(f"- Credit card usage: **{credit_pct:.1f}%**")
            weekend_avg_fare = df[df['is_weekend'] == 1]['fare_amount'].mean()
            weekday_avg_fare = df[df['is_weekend'] == 0]['fare_amount'].mean()
            st.write(f"- Weekend avg fare: **${weekend_avg_fare:.2f}** vs Weekday: **${weekday_avg_fare:.2f}**")
            st.markdown("</div>", unsafe_allow_html=True)
    
    # ============================================
    # PAGE 3: SQL INSIGHTS
    # ============================================
    elif page == "🔍 SQL Insights":
        st.header("SQL Analytics Engine")
        
        st.markdown("""
        Run analytical SQL queries on your trip data. Select from predefined queries or write your own!
        """)
        
        # Create SQL connection
        conn = create_sql_connection(df)
        
        # Predefined queries
        predefined_queries = {
            "Top 10 Revenue Days": """
                SELECT date, SUM(total_amount) as daily_revenue, COUNT(*) as trips
                FROM trips
                GROUP BY date
                ORDER BY daily_revenue DESC
                LIMIT 10
            """,
            "Peak Demand Hours": """
                SELECT hour_of_day, COUNT(*) as trip_count, 
                       AVG(fare_amount) as avg_fare,
                       SUM(total_amount) as total_revenue
                FROM trips
                GROUP BY hour_of_day
                ORDER BY trip_count DESC
            """,
            "Average Fare by Weekday": """
                SELECT day_name, 
                       COUNT(*) as trips,
                       AVG(fare_amount) as avg_fare,
                       AVG(trip_distance) as avg_distance,
                       AVG(tip_percentage) as avg_tip_pct
                FROM trips
                GROUP BY day_name
                ORDER BY 
                    CASE day_name
                        WHEN 'Monday' THEN 1
                        WHEN 'Tuesday' THEN 2
                        WHEN 'Wednesday' THEN 3
                        WHEN 'Thursday' THEN 4
                        WHEN 'Friday' THEN 5
                        WHEN 'Saturday' THEN 6
                        WHEN 'Sunday' THEN 7
                    END
            """,
            "Monthly Revenue Growth": """
                SELECT month_name, 
                       SUM(total_amount) as revenue,
                       COUNT(*) as trips,
                       AVG(total_amount) as avg_trip_value
                FROM trips
                GROUP BY month, month_name
                ORDER BY month
            """,
            "High-Value Trip Segments": """
                SELECT 
                    CASE 
                        WHEN total_amount < 10 THEN 'Low (<$10)'
                        WHEN total_amount < 30 THEN 'Medium ($10-$30)'
                        WHEN total_amount < 50 THEN 'High ($30-$50)'
                        ELSE 'Premium (>$50)'
                    END as trip_segment,
                    COUNT(*) as trip_count,
                    AVG(trip_distance) as avg_distance,
                    AVG(tip_percentage) as avg_tip_pct,
                    SUM(total_amount) as total_revenue
                FROM trips
                GROUP BY trip_segment
                ORDER BY total_revenue DESC
            """,
            "Peak vs Off-Peak Comparison": """
                SELECT 
                    CASE WHEN is_peak_hour = 1 THEN 'Peak' ELSE 'Off-Peak' END as period,
                    COUNT(*) as trips,
                    SUM(total_amount) as revenue,
                    AVG(fare_amount) as avg_fare,
                    AVG(trip_distance) as avg_distance
                FROM trips
                GROUP BY is_peak_hour
            """,
            "Time of Day Performance": """
                SELECT time_of_day,
                       COUNT(*) as trips,
                       SUM(total_amount) as revenue,
                       AVG(tip_percentage) as avg_tip_pct,
                       AVG(trip_duration_min) as avg_duration
                FROM trips
                GROUP BY time_of_day
                ORDER BY 
                    CASE time_of_day
                        WHEN 'Morning' THEN 1
                        WHEN 'Afternoon' THEN 2
                        WHEN 'Evening' THEN 3
                        WHEN 'Night' THEN 4
                    END
            """
        }
        
        query_type = st.radio(
            "Select Query Type",
            ["Predefined Queries", "Custom SQL"]
        )
        
        if query_type == "Predefined Queries":
            selected_query = st.selectbox(
                "Select Analysis",
                list(predefined_queries.keys())
            )
            
            query = predefined_queries[selected_query]
            st.code(query, language='sql')
            
            if st.button("Execute Query", type="primary"):
                try:
                    result = pd.read_sql_query(query, conn)
                    st.success(f"Query executed successfully! Retrieved {len(result)} rows.")
                    st.dataframe(result, use_container_width=True)
                    
                    # Download option
                    csv = result.to_csv(index=False)
                    st.download_button(
                        label="Download Results as CSV",
                        data=csv,
                        file_name=f"{selected_query.replace(' ', '_')}.csv",
                        mime="text/csv"
                    )
                except Exception as e:
                    st.error(f"Error executing query: {str(e)}")
        
        else:
            st.markdown("### Write Your Custom SQL Query")
            st.info("Available table: **trips** with all columns from your dataset")
            
            custom_query = st.text_area(
                "SQL Query",
                height=200,
                placeholder="SELECT * FROM trips LIMIT 10;"
            )
            
            col1, col2 = st.columns([1, 5])
            with col1:
                execute_btn = st.button("Execute", type="primary")
            
            if execute_btn and custom_query:
                try:
                    result = pd.read_sql_query(custom_query, conn)
                    st.success(f"Query executed successfully! Retrieved {len(result)} rows.")
                    st.dataframe(result, use_container_width=True)
                    
                    # Download option
                    csv = result.to_csv(index=False)
                    st.download_button(
                        label="Download Results as CSV",
                        data=csv,
                        file_name="custom_query_results.csv",
                        mime="text/csv"
                    )
                except Exception as e:
                    st.error(f"Error executing query: {str(e)}")
    
    # ============================================
    # PAGE 4: GenAI ASSISTANT
    # ============================================
    elif page == "🤖 GenAI Assistant":
        st.header("GenAI Urban Mobility Insights Assistant")
        
        st.markdown("""
        Ask questions about your transportation data in natural language!
        The AI assistant will analyze your data and provide insights.
        """)
        
        # Sample questions
        st.markdown("### 💡 Sample Questions")
        sample_questions = [
            "What were the busiest hours last month?",
            "How does revenue vary by day of week?",
            "When is surge demand highest?",
            "What's the average trip distance during peak hours?",
            "How do tips vary by time of day?",
            "Compare weekend vs weekday patterns"
        ]
        
        cols = st.columns(3)
        for idx, question in enumerate(sample_questions):
            with cols[idx % 3]:
                if st.button(question, key=f"sample_{idx}"):
                    st.session_state.selected_question = question
        
        st.markdown("---")
        
        # User input
        user_question = st.text_input(
            "Your Question",
            value=st.session_state.get('selected_question', ''),
            placeholder="Ask anything about your taxi trip data..."
        )
        
        if st.button("Get Insights", type="primary") and user_question:
            with st.spinner("Analyzing data..."):
                # Generate insights based on the question
                kpis = calculate_kpis(df)
                
                # Simple rule-based responses (can be replaced with actual GenAI API)
                response = generate_insights(user_question, df, kpis)
                
                st.markdown("<div class='insight-box'>", unsafe_allow_html=True)
                st.markdown("### 🤖 AI Response")
                st.write(response)
                st.markdown("</div>", unsafe_allow_html=True)
        
        # Monthly Executive Summary
        st.markdown("---")
        st.subheader("📋 Generate Executive Summary")
        
        if st.button("Generate Monthly Report", type="secondary"):
            summary = generate_executive_summary(df, kpis)
            
            st.markdown("<div class='insight-box'>", unsafe_allow_html=True)
            st.markdown(summary)
            st.markdown("</div>", unsafe_allow_html=True)
            
            # Download option
            st.download_button(
                label="Download Summary as Text",
                data=summary,
                file_name="executive_summary.txt",
                mime="text/plain"
            )
    
    # ============================================
    # PAGE 5: DATA EXPLORER
    # ============================================
    elif page == "📁 Data Explorer":
        st.header("Data Explorer")
        
        st.markdown("### Dataset Overview")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total Records", f"{len(df):,}")
        with col2:
            st.metric("Total Columns", len(df.columns))
        with col3:
            st.metric("Memory Usage", f"{df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
        
        # Data quality
        st.markdown("### Data Quality")
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### Missing Values")
            missing = df.isnull().sum()
            missing = missing[missing > 0]
            if len(missing) > 0:
                st.dataframe(missing, use_container_width=True)
            else:
                st.success("No missing values found!")
        
        with col2:
            st.markdown("#### Data Types")
            dtypes_df = pd.DataFrame({
                'Column': df.dtypes.index,
                'Type': df.dtypes.values.astype(str)
            })
            st.dataframe(dtypes_df, use_container_width=True)
        
        # Sample data
        st.markdown("### Sample Records")
        n_rows = st.slider("Number of rows to display", 5, 100, 10)
        st.dataframe(df.head(n_rows), use_container_width=True)
        
        # Statistical summary
        st.markdown("### Statistical Summary")
        st.dataframe(df.describe(), use_container_width=True)
        
        # Download cleaned data
        st.markdown("### Download Data")
        csv = df.to_csv(index=False)
        st.download_button(
            label="Download Cleaned Dataset",
            data=csv,
            file_name="cleaned_taxi_data.csv",
            mime="text/csv"
        )

else:
    # Welcome screen when no data is loaded
    st.info("👆 Please upload your cleaned_taxi_data.csv file using the sidebar to begin analysis")
    
    st.markdown("""
    ## Welcome to the Urban Mobility Analytics Platform! 🚖
    
    This platform provides comprehensive analytics for taxi trip data including:
    
    ### 📊 Dashboard
    - Real-time KPI monitoring
    - Revenue trends and patterns
    - Demand analysis by hour and day
    - Peak vs off-peak comparison
    
    ### 📈 Analytics
    - Deep revenue analysis
    - Spatial visualization
    - Temporal pattern detection
    - Automated insights generation
    
    ### 🔍 SQL Insights
    - Predefined analytical queries
    - Custom SQL query execution
    - Exportable results
    
    ### 🤖 GenAI Assistant
    - Natural language querying
    - Automated insights
    - Executive summaries
    
    ### 📁 Data Explorer
    - Dataset overview
    - Data quality checks
    - Statistical summaries
    
    **Get started by uploading your data file!**
    """)

# Helper functions for GenAI responses
def generate_insights(question, df, kpis):
    """Generate insights based on user question"""
    question_lower = question.lower()
    
    if 'busiest' in question_lower and 'hour' in question_lower:
        busiest_hour = df.groupby('hour_of_day').size().idxmax()
        trips_at_hour = df.groupby('hour_of_day').size().max()
        return f"The busiest hour is **{busiest_hour}:00** with **{trips_at_hour:,}** trips. This represents peak demand time when surge pricing could be most effective."
    
    elif 'revenue' in question_lower and 'day' in question_lower:
        revenue_by_day = df.groupby('day_name')['total_amount'].sum().sort_values(ascending=False)
        top_day = revenue_by_day.index[0]
        top_revenue = revenue_by_day.values[0]
        return f"**{top_day}** generates the highest revenue at **${top_revenue:,.2f}**. Revenue varies by day due to commuting patterns and weekend leisure travel."
    
    elif 'surge' in question_lower or 'peak' in question_lower:
        peak_hours = df[df['is_peak_hour'] == 1].groupby('hour_of_day').size().sort_values(ascending=False).head(3)
        peak_list = ', '.join([f"{h}:00" for h in peak_hours.index])
        return f"Surge demand is highest during **{peak_list}**. These hours coincide with rush hour commutes and typically see increased fares and reduced availability."
    
    elif 'distance' in question_lower and 'peak' in question_lower:
        peak_dist = df[df['is_peak_hour'] == 1]['trip_distance'].mean()
        off_peak_dist = df[df['is_peak_hour'] == 0]['trip_distance'].mean()
        return f"Average trip distance during peak hours is **{peak_dist:.2f} miles** compared to **{off_peak_dist:.2f} miles** during off-peak. Peak hour trips tend to be {'shorter' if peak_dist < off_peak_dist else 'longer'} due to commuting patterns."
    
    elif 'tip' in question_lower and 'time' in question_lower:
        tip_by_time = df.groupby('time_of_day')['tip_percentage'].mean().sort_values(ascending=False)
        best_time = tip_by_time.index[0]
        best_tip = tip_by_time.values[0]
        return f"Tips are highest during **{best_time}** with an average of **{best_tip:.1f}%**. Time of day significantly impacts tipping behavior, likely influenced by trip purpose and customer demographics."
    
    elif 'weekend' in question_lower or 'weekday' in question_lower:
        weekend_trips = df[df['is_weekend'] == 1].shape[0]
        weekday_trips = df[df['is_weekend'] == 0].shape[0]
        weekend_revenue = df[df['is_weekend'] == 1]['total_amount'].sum()
        weekday_revenue = df[df['is_weekend'] == 0]['total_amount'].sum()
        return f"**Weekday trips:** {weekday_trips:,} generating ${weekday_revenue:,.2f}\n**Weekend trips:** {weekend_trips:,} generating ${weekend_revenue:,.2f}\n\nWeekdays show higher volume due to commuting, while weekends may have longer leisure trips."
    
    else:
        return f"""Based on your data analysis:
        
- **Total trips analyzed:** {kpis['total_trips']:,}
- **Total revenue:** ${kpis['total_revenue']:,.2f}
- **Average fare:** ${kpis['avg_fare']:.2f}
- **Average trip distance:** {kpis['avg_distance']:.2f} miles
- **Average tip percentage:** {kpis['avg_tip_percentage']:.1f}%

For more specific insights, try asking about busiest hours, revenue patterns, peak demand, or comparing different time periods."""

def generate_executive_summary(df, kpis):
    """Generate executive summary report"""
    busiest_hour = df.groupby('hour_of_day').size().idxmax()
    busiest_day = df.groupby('day_name').size().idxmax()
    top_month = df.groupby('month_name')['total_amount'].sum().idxmax()
    peak_revenue_pct = (df[df['is_peak_hour'] == 1]['total_amount'].sum() / kpis['total_revenue']) * 100
    
    summary = f"""
# Executive Summary - Urban Mobility Analytics

## Overview
- **Reporting Period:** {df['date'].min().strftime('%B %d, %Y')} to {df['date'].max().strftime('%B %d, %Y')}
- **Total Trips:** {kpis['total_trips']:,}
- **Total Revenue:** ${kpis['total_revenue']:,.2f}

## Key Performance Indicators
- **Average Fare:** ${kpis['avg_fare']:.2f}
- **Average Trip Distance:** {kpis['avg_distance']:.2f} miles
- **Revenue per Mile:** ${kpis['revenue_per_mile']:.2f}
- **Average Trip Duration:** {kpis['avg_duration']:.1f} minutes
- **Average Tip Percentage:** {kpis['avg_tip_percentage']:.1f}%

## Demand Patterns
- **Busiest Hour:** {busiest_hour}:00
- **Busiest Day:** {busiest_day}
- **Highest Revenue Month:** {top_month}
- **Peak Hour Revenue Contribution:** {peak_revenue_pct:.1f}%

## Strategic Insights
1. **Peak Hour Optimization:** Peak hours contribute {peak_revenue_pct:.1f}% of total revenue, indicating strong demand during rush hours
2. **Distance Efficiency:** Average revenue per mile of ${kpis['revenue_per_mile']:.2f} suggests healthy pricing
3. **Customer Satisfaction:** Average tip percentage of {kpis['avg_tip_percentage']:.1f}% indicates good service quality
4. **Operational Efficiency:** Average trip duration of {kpis['avg_duration']:.1f} minutes shows efficient routing

## Recommendations
- Increase driver availability during peak hours ({busiest_hour}:00) to capture maximum demand
- Implement dynamic pricing during high-demand periods
- Focus marketing efforts on {busiest_day}s when demand is highest
- Monitor and maintain service quality to sustain tip percentages above industry average

---
*Generated by Urban Mobility Analytics Platform*
"""
    return summary