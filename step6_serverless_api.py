# serverless_api.py
"""
Serverless Urban Mobility Analytics API
Deployable on AWS Lambda or Azure Functions
"""

import json
import boto3
import pandas as pd
from datetime import datetime
import os

# AWS Configuration
S3_BUCKET = os.getenv('S3_BUCKET', 'taxi-analytics-data')
S3_PREFIX = os.getenv('S3_PREFIX', 'aggregated_kpis/')

class MobilityAnalyticsAPI:
    """
    Serverless API for Urban Mobility Analytics
    """
    
    def __init__(self):
        # Initialize S3 client (for AWS deployment)
        try:
            self.s3_client = boto3.client('s3')
            self.cloud_enabled = True
        except:
            self.cloud_enabled = False
            print("⚠ AWS credentials not configured. Running in local mode.")
        
        self.kpi_cache = {}
        self.cache_ttl = 3600  # 1 hour cache
    
    def load_kpis_from_s3(self, key):
        """Load pre-computed KPIs from S3"""
        if not self.cloud_enabled:
            return self._load_local_kpis(key)
        
        try:
            response = self.s3_client.get_object(
                Bucket=S3_BUCKET,
                Key=f"{S3_PREFIX}{key}"
            )
            data = json.loads(response['Body'].read())
            return data
        except Exception as e:
            return {"error": f"Failed to load KPIs: {str(e)}"}
    
    def _load_local_kpis(self, key):
        """Load KPIs from local file (for testing)"""
        try:
            with open(f"kpi_data/{key}", 'r') as f:
                return json.load(f)
        except:
            return self._get_mock_kpis(key)
    
    def _get_mock_kpis(self, key):
        """Generate mock KPI data for demo"""
        mock_data = {
            'monthly_revenue.json': {
                'January': 45632.50,
                'February': 42180.25,
                'March': 48920.75
            },
            'peak_hours.json': {
                '7': 245,
                '8': 312,
                '9': 289,
                '17': 398,
                '18': 421,
                '19': 367
            },
            'top_zones.json': {
                'Midtown': 1250,
                'Lower Manhattan': 980,
                'Upper Manhattan': 756,
                'Other': 542
            }
        }
        return mock_data.get(key, {})


# ============================================================================
# AWS LAMBDA HANDLER
# ============================================================================

def lambda_handler(event, context):
    """
    Main Lambda handler function
    Routes requests to appropriate endpoints
    """
    
    api = MobilityAnalyticsAPI()
    
    # Parse request
    http_method = event.get('httpMethod', 'GET')
    path = event.get('path', '/')
    query_params = event.get('queryStringParameters', {}) or {}
    
    # Route requests
    if path == '/monthly-revenue':
        return handle_monthly_revenue(api, query_params)
    
    elif path == '/peak-hours':
        return handle_peak_hours(api, query_params)
    
    elif path == '/top-zones':
        return handle_top_zones(api, query_params)
    
    elif path == '/health':
        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'status': 'healthy',
                'timestamp': datetime.now().isoformat(),
                'version': '1.0.0'
            })
        }
    
    else:
        return {
            'statusCode': 404,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'error': 'Endpoint not found',
                'available_endpoints': [
                    '/monthly-revenue',
                    '/peak-hours',
                    '/top-zones',
                    '/health'
                ]
            })
        }


def handle_monthly_revenue(api, params):
    """
    GET /monthly-revenue
    Returns monthly revenue statistics
    """
    try:
        data = api.load_kpis_from_s3('monthly_revenue.json')
        
        response = {
            'endpoint': '/monthly-revenue',
            'timestamp': datetime.now().isoformat(),
            'data': data,
            'summary': {
                'total_months': len(data),
                'total_revenue': sum(data.values()) if isinstance(data, dict) else 0,
                'avg_monthly_revenue': sum(data.values()) / len(data) if isinstance(data, dict) and data else 0
            }
        }
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps(response, indent=2)
        }
    
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'error': str(e)})
        }


def handle_peak_hours(api, params):
    """
    GET /peak-hours
    Returns peak hour demand statistics
    """
    try:
        data = api.load_kpis_from_s3('peak_hours.json')
        
        # Find busiest hour
        busiest_hour = max(data, key=data.get) if isinstance(data, dict) else None
        
        response = {
            'endpoint': '/peak-hours',
            'timestamp': datetime.now().isoformat(),
            'data': data,
            'insights': {
                'busiest_hour': busiest_hour,
                'busiest_hour_trips': data.get(busiest_hour, 0) if busiest_hour else 0,
                'peak_hours': ['7', '8', '9', '17', '18', '19']
            }
        }
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps(response, indent=2)
        }
    
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'error': str(e)})
        }


def handle_top_zones(api, params):
    """
    GET /top-zones
    Returns top pickup zones by demand
    """
    try:
        data = api.load_kpis_from_s3('top_zones.json')
        
        # Sort zones by trip count
        if isinstance(data, dict):
            sorted_zones = sorted(data.items(), key=lambda x: x[1], reverse=True)
        else:
            sorted_zones = []
        
        response = {
            'endpoint': '/top-zones',
            'timestamp': datetime.now().isoformat(),
            'data': data,
            'top_3': sorted_zones[:3] if sorted_zones else [],
            'total_zones': len(data) if isinstance(data, dict) else 0
        }
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps(response, indent=2)
        }
    
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'error': str(e)})
        }


# ============================================================================
# AZURE FUNCTIONS HANDLER
# ============================================================================

def azure_main(req):
    """
    Azure Functions main handler
    """
    import azure.functions as func
    
    api = MobilityAnalyticsAPI()
    
    # Parse request
    route = req.route_params.get('route', '')
    
    # Route to appropriate handler
    if route == 'monthly-revenue':
        result = handle_monthly_revenue(api, {})
    elif route == 'peak-hours':
        result = handle_peak_hours(api, {})
    elif route == 'top-zones':
        result = handle_top_zones(api, {})
    else:
        result = {
            'statusCode': 404,
            'body': json.dumps({'error': 'Route not found'})
        }
    
    return func.HttpResponse(
        result['body'],
        status_code=result['statusCode'],
        headers=result.get('headers', {})
    )


# ============================================================================
# LOCAL TESTING
# ============================================================================

def test_api_locally():
    """
    Test API endpoints locally before deployment
    """
    print("="*70)
    print("TESTING SERVERLESS API LOCALLY")
    print("="*70)
    
    test_events = [
        {
            'name': 'Monthly Revenue',
            'event': {
                'httpMethod': 'GET',
                'path': '/monthly-revenue',
                'queryStringParameters': {}
            }
        },
        {
            'name': 'Peak Hours',
            'event': {
                'httpMethod': 'GET',
                'path': '/peak-hours',
                'queryStringParameters': {}
            }
        },
        {
            'name': 'Top Zones',
            'event': {
                'httpMethod': 'GET',
                'path': '/top-zones',
                'queryStringParameters': {}
            }
        },
        {
            'name': 'Health Check',
            'event': {
                'httpMethod': 'GET',
                'path': '/health',
                'queryStringParameters': {}
            }
        }
    ]
    
    for test in test_events:
        print(f"\n{'='*70}")
        print(f"TEST: {test['name']}")
        print(f"{'='*70}")
        
        response = lambda_handler(test['event'], {})
        
        print(f"Status Code: {response['statusCode']}")
        print(f"Response Body:")
        print(json.dumps(json.loads(response['body']), indent=2))


# ============================================================================
# DEPLOYMENT SCRIPTS
# ============================================================================

def generate_aws_deployment_config():
    """Generate AWS SAM template for deployment"""
    
    template = """
# AWS SAM Template for Taxi Analytics API

AWSTemplateFormatVersion: '2010-09-09'
Transform: AWS::Serverless-2016-10-31
Description: Serverless Urban Mobility Analytics API

Resources:
  TaxiAnalyticsFunction:
    Type: AWS::Serverless::Function
    Properties:
      CodeUri: ./
      Handler: serverless_api.lambda_handler
      Runtime: python3.11
      MemorySize: 512
      Timeout: 30
      Environment:
        Variables:
          S3_BUCKET: taxi-analytics-data
          S3_PREFIX: aggregated_kpis/
      Policies:
        - S3ReadPolicy:
            BucketName: taxi-analytics-data
      Events:
        MonthlyRevenue:
          Type: Api
          Properties:
            Path: /monthly-revenue
            Method: GET
        PeakHours:
          Type: Api
          Properties:
            Path: /peak-hours
            Method: GET
        TopZones:
          Type: Api
          Properties:
            Path: /top-zones
            Method: GET
        Health:
          Type: Api
          Properties:
            Path: /health
            Method: GET

Outputs:
  ApiUrl:
    Description: "API Gateway endpoint URL"
    Value: !Sub "https://${ServerlessRestApi}.execute-api.${AWS::Region}.amazonaws.com/Prod/"
"""
    
    with open('template.yaml', 'w') as f:
        f.write(template)
    
    print("\n✓ Generated: template.yaml (AWS SAM)")
    
    # Generate deployment script
    deploy_script = """#!/bin/bash
# Deploy to AWS Lambda using SAM

echo "Building SAM application..."
sam build

echo "Deploying to AWS..."
sam deploy --guided

echo "Deployment complete!"
"""
    
    with open('deploy_aws.sh', 'w') as f:
        f.write(deploy_script)
    
    print("✓ Generated: deploy_aws.sh")


def generate_azure_deployment_config():
    """Generate Azure Functions configuration"""
    
    function_json = {
        "scriptFile": "serverless_api.py",
        "bindings": [
            {
                "authLevel": "function",
                "type": "httpTrigger",
                "direction": "in",
                "name": "req",
                "methods": ["get"],
                "route": "{route}"
            },
            {
                "type": "http",
                "direction": "out",
                "name": "$return"
            }
        ]
    }
    
    with open('function.json', 'w') as f:
        json.dump(function_json, f, indent=2)
    
    print("\n✓ Generated: function.json (Azure Functions)")
    
    # Generate requirements
    requirements = """
azure-functions
pandas
boto3
"""
    
    with open('requirements.txt', 'w') as f:
        f.write(requirements)
    
    print("✓ Generated: requirements.txt")


def print_deployment_guide():
    """Print deployment instructions"""
    
    guide = """
╔══════════════════════════════════════════════════════════════════════════╗
║                     CLOUD DEPLOYMENT GUIDE                               ║
╚══════════════════════════════════════════════════════════════════════════╝

AWS LAMBDA DEPLOYMENT
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Prerequisites:
1. Install AWS CLI: https://aws.amazon.com/cli/
2. Install AWS SAM CLI: https://aws.amazon.com/serverless/sam/
3. Configure AWS credentials: aws configure

Steps:
1. Upload aggregated KPIs to S3:
   aws s3 cp kpi_data/ s3://taxi-analytics-data/aggregated_kpis/ --recursive

2. Deploy Lambda function:
   sam build
   sam deploy --guided

3. Test endpoints:
   curl https://your-api-url/monthly-revenue
   curl https://your-api-url/peak-hours
   curl https://your-api-url/top-zones

AZURE FUNCTIONS DEPLOYMENT
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Prerequisites:
1. Install Azure CLI: https://docs.microsoft.com/cli/azure/
2. Install Azure Functions Core Tools
3. Login to Azure: az login

Steps:
1. Create Function App:
   az functionapp create --resource-group taxi-analytics \\
     --consumption-plan-location eastus \\
     --runtime python --runtime-version 3.11 \\
     --functions-version 4 --name taxi-analytics-api

2. Deploy function:
   func azure functionapp publish taxi-analytics-api

3. Test endpoints:
   curl https://taxi-analytics-api.azurewebsites.net/api/monthly-revenue

SCHEDULED EXECUTION
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

AWS CloudWatch Events (daily at 2 AM):
  - Create EventBridge rule with cron expression: cron(0 2 * * ? *)
  - Target: Your Lambda function

Azure Timer Trigger (daily at 2 AM):
  - Add timer trigger with CRON: 0 0 2 * * *

MONITORING & LOGGING
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

AWS:
  - CloudWatch Logs for function execution
  - CloudWatch Metrics for performance
  - X-Ray for distributed tracing

Azure:
  - Application Insights for monitoring
  - Log Analytics for querying logs
  - Azure Monitor for alerts

COST OPTIMIZATION
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

- Use S3/Blob Storage for caching aggregated KPIs
- Set appropriate memory/timeout limits
- Use reserved capacity for predictable workloads
- Implement API caching (API Gateway/Azure API Management)
- Monitor cold start times and optimize accordingly
"""
    
    print(guide)


# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    print("="*70)
    print("SERVERLESS URBAN MOBILITY ANALYTICS API")
    print("="*70)
    
    # Test API locally
    test_api_locally()
    
    # Generate deployment configs
    print("\n" + "="*70)
    print("GENERATING DEPLOYMENT CONFIGURATIONS")
    print("="*70)
    
    generate_aws_deployment_config()
    generate_azure_deployment_config()
    
    # Print deployment guide
    print_deployment_guide()
    
    print("\n✓ Serverless API setup complete!")
    print("\nNext steps:")
    print("1. Upload aggregated KPIs to cloud storage")
    print("2. Deploy using AWS SAM or Azure Functions CLI")
    print("3. Test endpoints and configure monitoring")
    print("4. Set up scheduled execution for daily updates")