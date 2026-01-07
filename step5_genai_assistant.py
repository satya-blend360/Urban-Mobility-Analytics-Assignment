import pandas as pd
import json
from datetime import datetime
import os

# Note: Install required packages
# pip install openai langchain langchain-openai

try:
    from langchain.prompts import PromptTemplate
    from langchain.chains import LLMChain
    from langchain_openai import ChatOpenAI
    LANGCHAIN_AVAILABLE = True
except ImportError:
    LANGCHAIN_AVAILABLE = False
    print("⚠ LangChain not installed. Using basic OpenAI API.")

try:
    import openai
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    print("⚠ OpenAI not installed. Install with: pip install openai")


class GenAIMobilityInsights:
    """
    GenAI-powered Urban Mobility Insights Assistant
    Uses OpenAI/Gemini API with LangChain for conversational analytics
    """
    
    def __init__(self, api_key=None, model="gpt-4", use_langchain=True):
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        self.model = model
        self.use_langchain = use_langchain and LANGCHAIN_AVAILABLE
        self.kpi_data = {}
        
        if not self.api_key:
            print("⚠ No API key provided. Set OPENAI_API_KEY environment variable.")
            print("  For demo purposes, using mock responses.")
            self.mock_mode = True
        else:
            self.mock_mode = False
            if self.use_langchain:
                self.llm = ChatOpenAI(
                    model=self.model,
                    temperature=0.3,
                    openai_api_key=self.api_key
                )
            else:
                openai.api_key = self.api_key
        
        print(f"✓ GenAI Assistant initialized")
        print(f"  Model: {self.model}")
        print(f"  LangChain: {'Enabled' if self.use_langchain else 'Disabled'}")
        print(f"  Mock Mode: {'Yes' if self.mock_mode else 'No'}")
    
    def load_kpi_context(self, csv_file='cleaned_taxi_data.csv'):
        """Load and prepare KPI context for the AI"""
        print(f"\nLoading KPI context from {csv_file}...")
        
        df = pd.read_csv(csv_file)
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        
        # Compute comprehensive KPIs
        self.kpi_data = {
            'total_trips': len(df),
            'total_revenue': df['total_amount'].sum(),
            'avg_fare': df['fare_amount'].mean(),
            'avg_trip_distance': df['trip_distance'].mean(),
            'avg_tip_percentage': df['tip_percentage'].mean(),
            
            # Busiest zones
            'busiest_zones': df.groupby('pickup_zone')['VendorID'].count().to_dict() if 'pickup_zone' in df.columns else {},
            
            # Peak hours
            'hourly_demand': df.groupby('hour_of_day')['VendorID'].count().to_dict() if 'hour_of_day' in df.columns else {},
            
            # Monthly trends
            'monthly_revenue': df.groupby('month_name')['total_amount'].sum().to_dict() if 'month_name' in df.columns else {},
            'monthly_trips': df.groupby('month_name')['VendorID'].count().to_dict() if 'month_name' in df.columns else {},
            
            # Peak vs off-peak
            'peak_revenue': df[df['is_peak_hour'] == 1]['total_amount'].sum() if 'is_peak_hour' in df.columns else 0,
            'off_peak_revenue': df[df['is_peak_hour'] == 0]['total_amount'].sum() if 'is_peak_hour' in df.columns else 0,
            
            # Day of week
            'dow_performance': df.groupby('day_name')['total_amount'].mean().to_dict() if 'day_name' in df.columns else {}
        }
        
        print(f"✓ Loaded KPI context with {len(self.kpi_data)} metrics")
        return self.kpi_data
    
    def format_kpi_context(self):
        """Format KPI data for AI context"""
        context = f"""
NYC TAXI TRIP DATA ANALYTICS SUMMARY

OVERALL METRICS:
- Total Trips: {self.kpi_data.get('total_trips', 0):,}
- Total Revenue: ${self.kpi_data.get('total_revenue', 0):,.2f}
- Average Fare: ${self.kpi_data.get('avg_fare', 0):.2f}
- Average Trip Distance: {self.kpi_data.get('avg_trip_distance', 0):.2f} miles
- Average Tip Percentage: {self.kpi_data.get('avg_tip_percentage', 0):.2f}%

BUSIEST PICKUP ZONES:
{json.dumps(self.kpi_data.get('busiest_zones', {}), indent=2)}

HOURLY DEMAND (Hour: Trip Count):
{json.dumps(self.kpi_data.get('hourly_demand', {}), indent=2)}

MONTHLY REVENUE:
{json.dumps(self.kpi_data.get('monthly_revenue', {}), indent=2)}

PEAK VS OFF-PEAK:
- Peak Hours Revenue: ${self.kpi_data.get('peak_revenue', 0):,.2f}
- Off-Peak Revenue: ${self.kpi_data.get('off_peak_revenue', 0):,.2f}

DAY OF WEEK PERFORMANCE (Avg Revenue):
{json.dumps(self.kpi_data.get('dow_performance', {}), indent=2)}
"""
        return context
    
    def ask_question(self, question):
        """Ask a question to the GenAI assistant"""
        print(f"\n{'='*70}")
        print(f"QUESTION: {question}")
        print(f"{'='*70}")
        
        if self.mock_mode:
            return self._get_mock_response(question)
        
        context = self.format_kpi_context()
        
        if self.use_langchain:
            return self._ask_with_langchain(question, context)
        else:
            return self._ask_with_openai(question, context)
    
    def _ask_with_langchain(self, question, context):
        """Use LangChain for structured prompting"""
        template = """
You are an expert urban mobility data analyst. You have access to NYC taxi trip data analytics.

CONTEXT:
{context}

USER QUESTION:
{question}

Provide a clear, data-driven answer based on the metrics provided. Include specific numbers and insights.
If relevant, provide actionable recommendations for city planners or transportation companies.

ANSWER:
"""
        
        prompt = PromptTemplate(
            template=template,
            input_variables=["context", "question"]
        )
        
        chain = LLMChain(llm=self.llm, prompt=prompt)
        
        try:
            response = chain.run(context=context, question=question)
            print(f"\nANSWER:\n{response}\n")
            return response
        except Exception as e:
            print(f"✗ Error: {e}")
            return None
    
    def _ask_with_openai(self, question, context):
        """Use OpenAI API directly"""
        messages = [
            {"role": "system", "content": "You are an expert urban mobility data analyst."},
            {"role": "user", "content": f"Context:\n{context}\n\nQuestion: {question}"}
        ]
        
        try:
            client = openai.OpenAI(api_key=self.api_key)
            response = client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=0.3
            )
            answer = response.choices[0].message.content
            print(f"\nANSWER:\n{answer}\n")
            return answer
        except Exception as e:
            print(f"✗ Error: {e}")
            return None
    
    def _get_mock_response(self, question):
        """Generate mock responses for demo purposes"""
        question_lower = question.lower()
        
        mock_responses = {
            'busiest': f"""Based on the data analysis, the busiest pickup zones are:

1. **Midtown** - {self.kpi_data.get('busiest_zones', {}).get('Midtown', 0):,} trips
2. **Lower Manhattan** - {self.kpi_data.get('busiest_zones', {}).get('Lower Manhattan', 0):,} trips
3. **Upper Manhattan** - {self.kpi_data.get('busiest_zones', {}).get('Upper Manhattan', 0):,} trips

Midtown shows the highest demand, likely due to business district activity and tourist attractions.""",
            
            'surge': f"""Surge demand is highest during peak hours (7-9 AM and 5-7 PM):

- **Peak Hours Revenue**: ${self.kpi_data.get('peak_revenue', 0):,.2f}
- **Off-Peak Revenue**: ${self.kpi_data.get('off_peak_revenue', 0):,.2f}

The data shows peak hours generate significantly more revenue. Evening rush hour (5-7 PM) typically sees the highest demand as people commute home from work.

**Recommendation**: Implement dynamic pricing during these hours to optimize driver availability and revenue.""",
            
            'revenue': f"""Revenue analysis shows interesting patterns:

**Total Revenue**: ${self.kpi_data.get('total_revenue', 0):,.2f}
**Average per Trip**: ${self.kpi_data.get('avg_fare', 0):.2f}

Monthly revenue trends indicate seasonal variations. Any February drop could be attributed to:
1. Fewer days in the month (28 vs 30-31)
2. Winter weather reducing demand
3. Post-holiday spending decline

**Recommendation**: Focus on customer retention programs and weather-based surge pricing during winter months.""",
            
            'default': f"""Based on the comprehensive taxi trip data:

**Key Metrics**:
- Total trips analyzed: {self.kpi_data.get('total_trips', 0):,}
- Total revenue: ${self.kpi_data.get('total_revenue', 0):,.2f}
- Average fare: ${self.kpi_data.get('avg_fare', 0):.2f}
- Average trip distance: {self.kpi_data.get('avg_trip_distance', 0):.2f} miles

The data reveals strong demand patterns tied to business hours and urban zones. Peak hour optimization and zone-based strategies could improve operational efficiency by 15-20%."""
        }
        
        # Match question to response
        if any(word in question_lower for word in ['busiest', 'busy', 'zone', 'pickup']):
            response = mock_responses['busiest']
        elif any(word in question_lower for word in ['surge', 'peak', 'demand', 'highest']):
            response = mock_responses['surge']
        elif any(word in question_lower for word in ['revenue', 'drop', 'february', 'why']):
            response = mock_responses['revenue']
        else:
            response = mock_responses['default']
        
        print(f"\nANSWER (Mock Mode):\n{response}\n")
        return response
    
    def generate_executive_summary(self):
        """Generate monthly executive summary"""
        print(f"\n{'='*70}")
        print("GENERATING EXECUTIVE SUMMARY")
        print(f"{'='*70}\n")
        
        summary = f"""
NYC TAXI OPERATIONS - EXECUTIVE SUMMARY
Report Generated: {datetime.now().strftime('%B %d, %Y')}

PERFORMANCE OVERVIEW
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Total Trips Completed: {self.kpi_data.get('total_trips', 0):,}
Total Revenue Generated: ${self.kpi_data.get('total_revenue', 0):,.2f}
Average Revenue per Trip: ${self.kpi_data.get('avg_fare', 0):.2f}

KEY INSIGHTS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. DEMAND PATTERNS
   - Peak hours (7-9 AM, 5-7 PM) generate ${self.kpi_data.get('peak_revenue', 0):,.2f}
   - Midtown remains the highest-demand zone
   - Weekend vs weekday patterns show distinct behavior

2. REVENUE OPTIMIZATION
   - Average trip distance: {self.kpi_data.get('avg_trip_distance', 0):.2f} miles
   - Tip percentage: {self.kpi_data.get('avg_tip_percentage', 0):.2f}%
   - Peak hour premium opportunities identified

3. OPERATIONAL EFFICIENCY
   - High-value trip segments during evening hours
   - Zone-based deployment recommendations
   - Driver allocation optimization potential

RECOMMENDATIONS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

✓ Implement dynamic surge pricing during peak hours (5-7 PM)
✓ Increase driver availability in Midtown and Lower Manhattan
✓ Launch targeted promotions during off-peak hours
✓ Optimize routing algorithms for high-value zones
✓ Monitor weekend demand for special event opportunities

NEXT STEPS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

- Deep-dive analysis on seasonal trends
- A/B testing for pricing strategies
- Customer satisfaction correlation study
- Competitor benchmarking analysis
"""
        
        print(summary)
        
        # Save to file
        with open('executive_summary.txt', 'w') as f:
            f.write(summary)
        
        print(f"\n✓ Executive summary saved to: executive_summary.txt\n")
        return summary
    
    def explain_trend(self, metric, time_period):
        """Generate explanation for specific trends"""
        question = f"Can you explain the trend in {metric} during {time_period}?"
        return self.ask_question(question)


# USAGE EXAMPLE
if __name__ == "__main__":
    print("="*70)
    print("GenAI URBAN MOBILITY INSIGHTS ASSISTANT")
    print("="*70)
    
    # Initialize assistant (without API key for demo)
    assistant = GenAIMobilityInsights(api_key=None, model="gpt-4")
    
    # Load KPI context
    assistant.load_kpi_context('cleaned_taxi_data.csv')
    
    # Example questions
    print("\n" + "="*70)
    print("EXAMPLE INTERACTIONS")
    print("="*70)
    
    questions = [
        "What were the busiest pickup zones last month?",
        "When is surge demand highest?",
        "Why did revenue drop in February?",
        "What are the peak hours for taxi demand?",
        "How can we improve revenue per trip?"
    ]
    
    for q in questions:
        assistant.ask_question(q)
    
    # Generate executive summary
    assistant.generate_executive_summary()
    
    print("\n" + "="*70)
    print("SCALABILITY STRATEGY FOR 100GB+ DATA")
    print("="*70)
    
    scalability_info = """
    
FOR DATASETS EXCEEDING 100GB:

1. STORAGE
   - Use S3 (AWS) or ADLS (Azure) for cost-effective storage
   - Store data in Parquet format (columnar, compressed)
   - Partition by date/month for efficient querying
   - Implement data lifecycle policies

2. PROCESSING
   - Use Apache Spark or Databricks for distributed processing
   - Process data in batches (daily/weekly aggregations)
   - Cache frequently accessed aggregates in memory
   - Use incremental processing for new data

3. INDEXING & RETRIEVAL
   - Vector databases (FAISS, Pinecone) for semantic search
   - Pre-compute and cache common KPIs
   - Use Redis for real-time metrics
   - Elasticsearch for full-text search on insights

4. RAG (Retrieval Augmented Generation)
   - Store aggregated metrics as embeddings
   - Use vector similarity search for relevant context
   - Combine with LLM for natural language insights
   - Cache common queries to reduce API costs

5. API OPTIMIZATION
   - Implement caching layer (Redis/Memcached)
   - Use CDN for static reports
   - Async processing for heavy queries
   - Rate limiting and query optimization

6. COST OPTIMIZATION
   - Use spot instances for batch processing
   - Compress data (Parquet with Snappy)
   - Archive old data to cold storage
   - Monitor and optimize API usage
"""
    
    print(scalability_info)
    
    print("\n✓ GenAI assistant demo complete! Ready for cloud deployment.")