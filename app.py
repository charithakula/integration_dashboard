import os
import time
import json
import threading
from datetime import datetime, timedelta
from functools import wraps
from typing import Optional, Dict, Any, List
import io

# Flask 3.1.2 LTS
from flask import Flask, render_template, jsonify, request, send_file
from flask_cors import CORS
from werkzeug.exceptions import BadRequest, NotFound, InternalServerError

# Environment Management
from dotenv import load_dotenv

# Data Processing
import pandas as pd
import numpy as np

# PDF Generation
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import colors

# Task Scheduling
import schedule

# HTTP Requests
import requests

# Validation
from marshmallow import Schema, fields, ValidationError

# Monitoring (Prometheus)
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST

# Structured Logging
import structlog

# Date/Time Utilities
from dateutil import parser as date_parser
from dateutil.relativedelta import relativedelta

# Load environment variables
load_dotenv()

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()

# Prometheus Metrics
REQUEST_COUNT = Counter('flask_requests_total', 'Total Flask requests', ['method', 'endpoint'])
REQUEST_DURATION = Histogram('flask_request_duration_seconds', 'Flask request duration')
SPLUNK_QUERY_COUNT = Counter('splunk_queries_total', 'Total Splunk queries', ['query_type', 'status'])
CACHE_HIT_COUNT = Counter('cache_hits_total', 'Cache hits', ['cache_type'])
ACTIVE_CONNECTIONS = Gauge('active_connections', 'Active connections')

# Initialize Flask App
app = Flask(__name__)

# Configuration - Token Authentication Only
class Config:
    SECRET_KEY = os.getenv('SECRET_KEY', 'dev-secret-key-change-in-production')
    
    # Splunk Configuration - Token Authentication Only
    SPLUNK_HOST = os.getenv('SPLUNK_HOST', 'localhost')
    SPLUNK_PORT = int(os.getenv('SPLUNK_PORT', '8089'))
    SPLUNK_SCHEME = os.getenv('SPLUNK_SCHEME', 'https')
    SPLUNK_AUTH_TOKEN = os.getenv('SPLUNK_AUTH_TOKEN')
    
    # Cache Configuration (In-Memory Only)
    CACHE_TTL = int(os.getenv('CACHE_TTL', '300'))  # 5 minutes
    
    # Application Settings
    DEBUG = os.getenv('FLASK_DEBUG', 'False').lower() == 'true'
    PORT = int(os.getenv('PORT', '5000'))
    HOST = os.getenv('HOST', '0.0.0.0')

app.config.from_object(Config)
CORS(app)

# In-memory cache ONLY
memory_cache = {}
logger.info("Initialized in-memory caching", ttl=f"{app.config['CACHE_TTL']}s")

# Validation Schemas
class DashboardQuerySchema(Schema):
    time_range = fields.Str(missing='30d', validate=lambda x: x in ['1d', '7d', '30d', '90d'])
    environment = fields.Str(missing='prod', validate=lambda x: x in ['prod', 'staging', 'dev'])
    api_filter = fields.Str(missing='')

class CustomQuerySchema(Schema):
    query = fields.Str(required=True)
    time_range = fields.Str(missing='30d')
    environment = fields.Str(missing='prod')

# Simplified SplunkConnector - Token Authentication Only
class SplunkConnector:
    def __init__(self, config: Dict[str, Any]):
        self.config = {
            'host': config['SPLUNK_HOST'],
            'port': config['SPLUNK_PORT'],
            'scheme': config['SPLUNK_SCHEME'],
            'auth_token': config.get('SPLUNK_AUTH_TOKEN')
        }
        self.base_url = f"{self.config['scheme']}://{self.config['host']}:{self.config['port']}"
        self.connection_attempts = 0
        self.max_retries = 3
        self.last_connection_attempt = None
        self.connection_cooldown = 30
        
        # Create a requests session for connection pooling
        self.session = requests.Session()
        self.session.verify = False  # Set to True in production with proper SSL
        
        # Validate token exists
        if not self.config.get('auth_token'):
            logger.warning("No SPLUNK_AUTH_TOKEN provided - will use mock data")
        else:
            logger.info("SplunkConnector initialized with token authentication", host=self.config['host'])
    
    def should_attempt_connection(self):
        """Check if we should attempt a connection based on cooldown"""
        if self.last_connection_attempt is None:
            return True
        return time.time() - self.last_connection_attempt > self.connection_cooldown
    
    def test_connection(self):
        """Test token authentication by running a simple search"""
        if not self.config.get('auth_token'):
            logger.warning("No auth token available for connection test")
            return False
            
        if not self.should_attempt_connection():
            logger.debug("Connection test skipped due to cooldown")
            return False
            
        self.last_connection_attempt = time.time()
        
        try:
            search_url = f"{self.base_url}/services/search/jobs"
            
            search_data = {
                'search': '| stats count',
                'output_mode': 'json',
                'exec_mode': 'blocking',
                'timeout': 10
            }
            
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
                'Authorization': f'Bearer {self.config["auth_token"]}'
            }
            
            logger.info("Testing Splunk connection with token")
            
            response = self.session.post(
                search_url,
                data=search_data,
                headers=headers,
                timeout=15
            )
            
            response.raise_for_status()
            search_result = response.json()
            
            if 'sid' in search_result:
                logger.info("Token authentication test successful", sid=search_result['sid'])
                self.connection_attempts = 0
                return True
            else:
                logger.error("Token test failed - no SID returned")
                return False
                
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                logger.error("Token authentication failed - invalid token")
            else:
                logger.error("HTTP error during connection test", status=e.response.status_code, error=str(e))
            self.connection_attempts += 1
            return False
        except Exception as e:
            logger.error("Connection test failed", error=str(e))
            self.connection_attempts += 1
            return False
    
    def connect(self):
        """Test connection - simplified since tokens don't require session setup"""
        if not self.config.get('auth_token'):
            logger.warning("No auth token provided, skipping connection")
            return False
            
        while self.connection_attempts < self.max_retries:
            if self.test_connection():
                ACTIVE_CONNECTIONS.inc()
                logger.info("Successfully connected to Splunk", host=self.config['host'])
                return True
            
            if self.connection_attempts < self.max_retries:
                wait_time = min(2 ** self.connection_attempts, 10)
                logger.info(f"Connection failed, retrying in {wait_time}s", attempt=self.connection_attempts + 1)
                time.sleep(wait_time)
        
        logger.error("Failed to connect to Splunk after all retries", max_retries=self.max_retries)
        return False
    
    def execute_query(self, query: str, earliest_time: str = "-30d@d", 
                     latest_time: str = "now", timeout: int = 30) -> List[Dict]:
        """Execute query using search endpoint with token authentication"""
        
        if not self.config.get('auth_token'):
            logger.warning("No auth token available for query execution")
            return []
        
        start_time = time.time()
        
        try:
            # Security validation
            dangerous_keywords = ['delete', 'drop', 'insert', 'update', 'create', 'alter', 'eval', 'outputlookup']
            if any(keyword in query.lower() for keyword in dangerous_keywords):
                raise ValueError(f"Query contains potentially dangerous operations")
            
            # Use the working search endpoint
            search_url = f"{self.base_url}/services/search/jobs"
            
            # Prepare the search query
            full_query = f"search {query} earliest={earliest_time} latest={latest_time}"
            
            search_data = {
                'search': full_query,
                'output_mode': 'json',
                'exec_mode': 'blocking',
                'timeout': timeout
            }
            
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
                'Authorization': f'Bearer {self.config["auth_token"]}'
            }
            
            logger.info("Executing Splunk query", 
                       query_preview=query[:50],
                       earliest_time=earliest_time,
                       latest_time=latest_time)
            
            # Create search job with blocking execution
            response = self.session.post(
                search_url,
                data=search_data,
                headers=headers,
                timeout=timeout + 5
            )
            
            response.raise_for_status()
            search_result = response.json()
            
            # Extract job SID from response
            if 'sid' not in search_result:
                logger.error("No SID in search response", response=search_result)
                SPLUNK_QUERY_COUNT.labels(query_type='error', status='no_sid').inc()
                return []
            
            job_sid = search_result['sid']
            logger.info("Search job created successfully", job_sid=job_sid)
            
            # Get results using the SID
            results_url = f"{self.base_url}/services/search/jobs/{job_sid}/results"
            results_params = {
                'output_mode': 'json',
                'count': 1000
            }
            
            results_response = self.session.get(
                results_url,
                params=results_params,
                headers=headers,
                timeout=15
            )
            
            results_response.raise_for_status()
            results_data = results_response.json()
            
            # Extract results from response
            results_list = []
            if 'results' in results_data:
                results_list = results_data['results']
            
            execution_time = time.time() - start_time
            logger.info("Query completed successfully", 
                       results_count=len(results_list),
                       execution_time=f"{execution_time:.2f}s",
                       job_sid=job_sid)
            
            SPLUNK_QUERY_COUNT.labels(query_type='success', status='completed').inc()
            REQUEST_DURATION.observe(execution_time)
            
            return results_list
            
        except requests.exceptions.Timeout as e:
            execution_time = time.time() - start_time
            logger.error("Query timeout", 
                       error=str(e),
                       execution_time=f"{execution_time:.2f}s",
                       timeout=timeout)
            SPLUNK_QUERY_COUNT.labels(query_type='error', status='timeout').inc()
            return []
            
        except requests.exceptions.HTTPError as e:
            execution_time = time.time() - start_time
            
            if e.response.status_code == 401:
                logger.warning("Authentication error - invalid or expired token", error=str(e))
                SPLUNK_QUERY_COUNT.labels(query_type='error', status='auth_failed').inc()
            elif e.response.status_code == 403:
                logger.error("Permission denied - token lacks required permissions", error=str(e))
                SPLUNK_QUERY_COUNT.labels(query_type='error', status='permission_denied').inc()
            else:
                logger.error("HTTP error executing Splunk query", 
                           error=str(e),
                           status_code=e.response.status_code,
                           execution_time=f"{execution_time:.2f}s")
                SPLUNK_QUERY_COUNT.labels(query_type='error', status='http_error').inc()
            
            return []
            
        except Exception as e:
            execution_time = time.time() - start_time
            error_type = type(e).__name__
            
            logger.error("Error executing Splunk query", 
                       error=str(e),
                       error_type=error_type,
                       execution_time=f"{execution_time:.2f}s")
            SPLUNK_QUERY_COUNT.labels(query_type='error', status='query_failed').inc()
            
            return []
    
    def get_session_status(self):
        """Get current authentication status"""
        return {
            'auth_method': 'token',
            'has_auth_token': bool(self.config.get('auth_token')),
            'token_prefix': self.config['auth_token'][:10] + "..." if self.config.get('auth_token') else None,
            'connection_attempts': self.connection_attempts,
            'service_connected': self.connection_attempts < self.max_retries,
            'last_connection_attempt': self.last_connection_attempt
        }

# Initialize Splunk connector
try:
    splunk = SplunkConnector(app.config)
except Exception as e:
    logger.error("Failed to initialize SplunkConnector", error=str(e))
    splunk = None

# Enhanced cache functions with better key management
def get_cache_key(prefix: str, **kwargs) -> str:
    """Generate cache key from parameters"""
    sorted_params = sorted(kwargs.items())
    params_str = "_".join([f"{k}:{v}" for k, v in sorted_params])
    return f"{prefix}:{params_str}"

def get_from_cache(key: str) -> Optional[Any]:
    """Get value from in-memory cache with better error handling"""
    try:
        if key in memory_cache:
            data, timestamp = memory_cache[key]
            if time.time() - timestamp < app.config['CACHE_TTL']:
                CACHE_HIT_COUNT.labels(cache_type='memory').inc()
                logger.debug("Cache hit", key=key)
                return data
            else:
                # Remove expired entry
                del memory_cache[key]
                logger.debug("Cache expired, removed", key=key)
    except Exception as e:
        logger.error("Cache retrieval error", error=str(e), key=key)
    
    return None

def set_cache(key: str, value: Any, ttl: int = None) -> bool:
    """Set value in in-memory cache"""
    try:
        memory_cache[key] = (value, time.time())
        logger.debug("Cache stored", key=key)
        return True
    except Exception as e:
        logger.error("Cache storage error", error=str(e), key=key)
        return False

def clear_cache():
    """Clear all cache entries"""
    try:
        cleared_count = len(memory_cache)
        memory_cache.clear()
        logger.info("Cache cleared", entries_removed=cleared_count)
        return cleared_count
    except Exception as e:
        logger.error("Cache clear error", error=str(e))
        return 0

# Enhanced caching decorator with mock fallback
def cache_result(cache_prefix: str, ttl: int = None, use_mock_on_failure: bool = True):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Create cache key from function arguments
            cache_key = get_cache_key(cache_prefix, **kwargs)
            
            # Try to get from cache
            cached_result = get_from_cache(cache_key)
            if cached_result is not None:
                logger.debug("Cache hit", function=func.__name__, cache_key=cache_key)
                return cached_result
            
            # Execute function and cache result
            try:
                result = func(*args, **kwargs)
                if result:
                    set_cache(cache_key, result, ttl or app.config['CACHE_TTL'])
                    logger.debug("Cache miss - stored result", function=func.__name__, cache_key=cache_key)
                    return result
                elif use_mock_on_failure:
                    logger.warning("Function returned empty result, using mock data", function=func.__name__)
                    mock_result = get_mock_data()
                    return mock_result
                else:
                    return result
            except Exception as e:
                logger.error("Function execution failed", function=func.__name__, error=str(e))
                if use_mock_on_failure:
                    return get_mock_data()
                return {}
        return wrapper
    return decorator

# Request middleware for metrics
@app.before_request
def before_request():
    request.start_time = time.time()
    REQUEST_COUNT.labels(method=request.method, endpoint=request.endpoint or 'unknown').inc()

@app.after_request
def after_request(response):
    if hasattr(request, 'start_time'):
        duration = time.time() - request.start_time
        REQUEST_DURATION.observe(duration)
    return response

# Splunk Queries for Dashboard
SPLUNK_QUERIES = {
    'total_consumers': '''
        index="datapower" sourcetype="datapower:apimgr:app" catalog_name="prod" 
        app_name!="N/A" app_name!="" *INTERNAL*OS_* 
        | stats distinct_count(app_name)
    ''',
    
    'top_apis_by_consumers': '''
        index="datapower" sourcetype="datapower:apimgr:app" catalog_name="prod" 
        app_name!="N/A" app_name!="" *INTERNAL*OS_* 
        | stats distinct_count(app_name) as consumers by api_name 
        | sort - consumers 
        | head 10
    ''',
    
    'api_usage_count': '''
        index="datapower" sourcetype="datapower:apimgr" catalog_name="prod" 
        app_name!="N/A" app_name!="" *INTERNAL*OS_* 
        | stats distinct_count(app_name) as AppUsageCount by api_name 
        | sort - AppUsageCount 
        | head 10
    ''',
    
    'api_usage_by_app': '''
        index="datapower" sourcetype="datapower:apimgr:app" catalog_name="prod" 
        app_name!="N/A" app_name!="" *INTERNAL*OS_* app_name!="API-MSP" 
        app_name!="API Connect Common App" app_name!="developer" app_name!="N/A" 
        | stats distinct_count(api_name) as api_usage by app_name 
        | sort - api_usage 
        | head 10
    ''',
    
    'performance_metrics_7d': '''
        index="datapower" sourcetype="datapower:apimgr:app" catalog_name="prod" 
        status_code="20*" app_name!="N/A" app_name!="" *INTERNAL*OS_* 
        earliest=-7d@d latest=@m 
        | lookup icoe_api_reporting_dashboard_providerorg api_name 
        | stats min(time_to_serve_request) as min_response 
          avg(time_to_serve_request) as avg_response 
          max(time_to_serve_request) as max_response 
          count(resource_id) as total_count by ou_name api_name 
        | sort - total_count
    ''',
    
    'performance_metrics_30d': '''
        index="datapower" sourcetype="datapower:apimgr:app" catalog_name="prod" 
        status_code="20*" app_name!="N/A" app_name!="" *INTERNAL*OS_* 
        earliest=-30d@d latest=@m 
        | lookup icoe_api_reporting_dashboard_providerorg api_name 
        | stats min(time_to_serve_request) as min_response 
          avg(time_to_serve_request) as avg_response 
          max(time_to_serve_request) as max_response 
          count(resource_id) as total_count by ou_name api_name 
        | sort - total_count
    ''',
    
    'ou_api_usage': '''
        index="datapower" sourcetype="datapower:apimgr:app" catalog_name="prod" 
        app_name!="N/A" app_name!="" *INTERNAL*OS_* 
        | lookup icoe_api_reporting_dashboard_providerorg api_name 
        | stats distinct_count(api_name) as api_usage by ou_name
    ''',
    
    'ou_api_usage_filtered': '''
        index="datapower" sourcetype="datapower:apimgr:app" catalog_name="prod" 
        app_name!="N/A" app_name!="" *INTERNAL*OS_* app_name!="API Connect Common App" 
        | lookup icoe_api_reporting_dashboard_providerorg app_name 
        | stats distinct_count(api_name) as APIUsageCount by ou_name
    ''',
    
    'performance_timechart': '''
        index="datapower" sourcetype="datapower:apimgr:app" catalog_name="prod" 
        (api_name="payments" OR api_name="viewusage" OR api_name="bills") 
        earliest=-30d@d latest=@m 
        | timechart span=1h avg(time_to_serve_request) by api_name
    ''',
    
    'daily_app_usage': '''
        index="datapower" sourcetype="datapower:apimgr:app" catalog_name="prod" 
        app_name!="N/A" app_name!="API Connect Common App" 
        | timechart span=1d count by app_name limit=5
    ''',
    
    'error_analysis': '''
        index="datapower" sourcetype="datapower:apimgr:app" catalog_name="prod" 
        status_code="40*" OR status_code="50*"
        | stats count(uri_path) as CountOfPath by uri_path status_code bytes_sent
        | sort - CountOfPath
        | head 20
    ''',
    
    'api_reusability': '''
        index="datapower" sourcetype="datapower:apimgr:app" catalog_name="prod" 
        app_name!="N/A" app_name!="" *INTERNAL*OS_* 
        | stats distinct_count(api_name) as "used"
    '''
}

# Enhanced mock data function with more realistic data
def get_mock_data():
    """Enhanced mock data for development and fallback"""
    return {
        'top_apis_by_consumers': {
            'labels': ['payments', 'authentication', 'customer', 'billing', 'inventory', 
                      'notifications', 'analytics', 'reporting', 'search', 'upload'],
            'data': [245, 198, 167, 134, 89, 76, 65, 54, 43, 32]
        },
        'ou_api_usage': {
            'labels': ['Finance', 'Marketing', 'Operations', 'HR', 'IT', 'Sales'],
            'data': [156, 134, 98, 67, 156, 89]
        },
        'performance_timechart': {
            'labels': ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'],
            'datasets': [
                {
                    'label': 'Payments API',
                    'data': [250, 245, 260, 255, 240, 230, 235],
                    'borderColor': '#3b82f6',
                    'backgroundColor': 'rgba(59, 130, 246, 0.1)',
                    'tension': 0.4,
                    'fill': True
                },
                {
                    'label': 'Authentication API',
                    'data': [145, 150, 148, 152, 155, 150, 153],
                    'borderColor': '#10b981',
                    'backgroundColor': 'rgba(16, 185, 129, 0.1)',
                    'tension': 0.4,
                    'fill': True
                }
            ]
        },
        'daily_app_usage': {
            'labels': ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'],
            'datasets': [
                {
                    'label': 'Mobile Applications',
                    'data': [120000, 135000, 140000, 125000, 160000, 145000, 155000],
                    'backgroundColor': '#3b82f6'
                },
                {
                    'label': 'Web Applications',
                    'data': [85000, 92000, 88000, 95000, 110000, 98000, 105000],
                    'backgroundColor': '#10b981'
                }
            ]
        },
        'error_analysis': {
            'labels': ['400 Bad Request', '401 Unauthorized', '403 Forbidden', '404 Not Found', '429 Rate Limited', '500 Server Error'],
            'data': [23, 18, 12, 15, 8, 4]
        },
        'stats': {
            'total_apis': '247',
            'total_consumers': '156',
            'avg_response_time': '245ms',
            'total_requests': '2.4M',
            'error_rate': '0.12%',
            'reuse_score': '87%'
        },
        'metadata': {
            'timestamp': datetime.now().isoformat(),
            'environment': 'mock',
            'time_range': '30d',
            'query_count': 6,
            'cache_status': 'mock_data'
        }
    }

# Enhanced data processing with better error handling
def process_splunk_data(raw_data: List[Dict], query_type: str) -> Dict[str, Any]:
    """Enhanced data processing with pandas for better performance"""
    try:
        if not raw_data:
            logger.warning("No data received for processing", query_type=query_type)
            return {'labels': [], 'data': []}
        
        # Convert to DataFrame for easier processing
        df = pd.DataFrame(raw_data)
        
        if query_type == 'top_apis_by_consumers':
            if 'api_name' in df.columns and 'consumers' in df.columns:
                df['consumers'] = pd.to_numeric(df['consumers'], errors='coerce').fillna(0)
                df = df.sort_values('consumers', ascending=False).head(10)
                return {
                    'labels': df['api_name'].tolist(),
                    'data': df['consumers'].astype(int).tolist()
                }
        
        elif query_type == 'ou_api_usage':
            if 'ou_name' in df.columns and 'api_usage' in df.columns:
                df['api_usage'] = pd.to_numeric(df['api_usage'], errors='coerce').fillna(0)
                return {
                    'labels': df['ou_name'].tolist(),
                    'data': df['api_usage'].astype(int).tolist()
                }
        
        elif query_type == 'performance_timechart':
            if '_time' in df.columns and 'api_name' in df.columns:
                # Process time series data
                df['_time'] = pd.to_datetime(df['_time'], errors='coerce')
                df = df.dropna(subset=['_time'])
                
                datasets = []
                colors = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6']
                
                for i, api_name in enumerate(df['api_name'].unique()):
                    api_data = df[df['api_name'] == api_name].copy()
                    api_data = api_data.sort_values('_time')
                    
                    datasets.append({
                        'label': api_name,
                        'data': api_data['avg(time_to_serve_request)'].astype(float).tolist(),
                        'borderColor': colors[i % len(colors)],
                        'backgroundColor': colors[i % len(colors)] + '20',
                        'tension': 0.4,
                        'fill': False
                    })
                
                return {
                    'labels': sorted(df['_time'].dt.strftime('%Y-%m-%d %H:%M').unique()),
                    'datasets': datasets
                }
        
        elif query_type == 'error_analysis':
            if 'status_code' in df.columns and 'CountOfPath' in df.columns:
                df['CountOfPath'] = pd.to_numeric(df['CountOfPath'], errors='coerce').fillna(0)
                grouped = df.groupby('status_code')['CountOfPath'].sum().reset_index()
                grouped = grouped.sort_values('CountOfPath', ascending=False)
                
                return {
                    'labels': grouped['status_code'].tolist(),
                    'data': grouped['CountOfPath'].astype(int).tolist()
                }
        
        # Generic processing for other query types
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        text_cols = df.select_dtypes(include=['object']).columns
        
        if len(text_cols) > 0 and len(numeric_cols) > 0:
            label_col = text_cols[0]
            value_col = numeric_cols[0]
            
            return {
                'labels': df[label_col].tolist(),
                'data': df[value_col].astype(int).tolist()
            }
        
        return {'labels': [], 'data': []}
        
    except Exception as e:
        logger.error("Error processing Splunk data", error=str(e), query_type=query_type)
        return {'labels': [], 'data': []}

# Enhanced dashboard data function with better fallback
@cache_result('dashboard_data', ttl=300, use_mock_on_failure=True)
def get_dashboard_data(time_range: str = '30d', environment: str = 'prod') -> Dict[str, Any]:
    """Enhanced dashboard data fetching with better error handling and automatic fallback"""
    try:
        dashboard_data = {}
        earliest_time = f"-{time_range}@d"
        
        # Check if we have a Splunk connector and can connect
        if not splunk or not splunk.config.get('auth_token'):
            logger.warning("No Splunk connection available, using mock data")
            return get_mock_data()
        
        # Execute key queries with progress tracking
        key_queries = [
            'top_apis_by_consumers',
            'ou_api_usage', 
            'performance_timechart',
            'daily_app_usage',
            'error_analysis',
            'total_consumers'
        ]
        
        successful_queries = 0
        for query_name in key_queries:
            if query_name in SPLUNK_QUERIES:
                query = SPLUNK_QUERIES[query_name]
                
                # Environment-specific query modification
                if environment != 'prod':
                    query = query.replace('catalog_name="prod"', f'catalog_name="{environment}"')
                
                # Time range modification
                if time_range != '30d':
                    query = query.replace('earliest=-30d@d', earliest_time)
                    query = query.replace('earliest=-7d@d', earliest_time)
                
                logger.info("Executing query", query_name=query_name, environment=environment, time_range=time_range)
                
                try:
                    result = splunk.execute_query(query, earliest_time, timeout=20)
                    processed_result = process_splunk_data(result, query_name)
                    
                    # Only add result if it has actual data
                    if processed_result and (processed_result.get('labels') or processed_result.get('datasets')):
                        dashboard_data[query_name] = processed_result
                        successful_queries += 1
                    else:
                        logger.warning("Query returned no useful data", query_name=query_name)
                        
                except Exception as query_error:
                    logger.error("Query execution failed", query_name=query_name, error=str(query_error))
        
        # If we got some data, continue; if no queries succeeded, use mock data
        if successful_queries == 0:
            logger.warning("All Splunk queries failed, using mock data")
            return get_mock_data()
        
        # Calculate enhanced statistics
        stats = calculate_summary_stats(dashboard_data)
        dashboard_data['stats'] = stats
        
        # Add metadata
        dashboard_data['metadata'] = {
            'timestamp': datetime.now().isoformat(),
            'environment': environment,
            'time_range': time_range,
            'query_count': len(key_queries),
            'successful_queries': successful_queries,
            'cache_status': 'miss',
            'data_source': 'splunk'
        }
        
        return dashboard_data
        
    except Exception as e:
        logger.error("Error fetching dashboard data", error=str(e))
        return get_mock_data()

def calculate_summary_stats(dashboard_data: Dict[str, Any]) -> Dict[str, Any]:
    """Enhanced statistics calculation with numpy for better performance"""
    try:
        stats = {
            'total_apis': 0,
            'total_consumers': 0,
            'avg_response_time': '0ms',
            'total_requests': '0',
            'error_rate': '0.00%',
            'reuse_score': '0%'
        }
        
        # Calculate total APIs
        if 'top_apis_by_consumers' in dashboard_data:
            stats['total_apis'] = len(dashboard_data['top_apis_by_consumers'].get('labels', []))
        
        # Calculate total consumers using numpy for better performance
        if 'top_apis_by_consumers' in dashboard_data:
            consumers_data = dashboard_data['top_apis_by_consumers'].get('data', [])
            if consumers_data:
                stats['total_consumers'] = int(np.sum(consumers_data))
        
        # Calculate average response time if available
        if 'performance_timechart' in dashboard_data:
            perf_data = dashboard_data['performance_timechart']
            if 'datasets' in perf_data:
                all_response_times = []
                for dataset in perf_data['datasets']:
                    all_response_times.extend(dataset.get('data', []))
                if all_response_times:
                    avg_time = np.mean(all_response_times)
                    stats['avg_response_time'] = f"{avg_time:.0f}ms"
        
        # Enhanced request calculation
        base_requests = stats['total_consumers'] * 1500
        stats['total_requests'] = f"{base_requests/1000:.1f}K" if base_requests < 1000000 else f"{base_requests/1000000:.1f}M"
        
        # Calculate error rate
        if 'error_analysis' in dashboard_data:
            error_data = dashboard_data['error_analysis'].get('data', [])
            if error_data:
                total_errors = sum(error_data)
                error_rate = (total_errors / max(base_requests, 1)) * 100
                stats['error_rate'] = f"{error_rate:.3f}%"
        
        # Enhanced reusability score
        if 'top_apis_by_consumers' in dashboard_data:
            consumers_data = dashboard_data['top_apis_by_consumers'].get('data', [])
            if consumers_data:
                consumers_array = np.array(consumers_data)
                high_reuse = np.sum(consumers_array > 10)
                medium_reuse = np.sum((consumers_array >= 5) & (consumers_array <= 10))
                total_apis = len(consumers_array)
                
                if total_apis > 0:
                    reuse_score = ((high_reuse * 1.0) + (medium_reuse * 0.6)) / total_apis * 100
                    stats['reuse_score'] = f"{reuse_score:.0f}%"
        
        return stats
        
    except Exception as e:
        logger.error("Error calculating summary stats", error=str(e))
        return {
            'total_apis': '247',
            'total_consumers': '156', 
            'avg_response_time': '245ms',
            'total_requests': '2.4M',
            'error_rate': '0.12%',
            'reuse_score': '87%'
        }

# Enhanced Routes with validation and monitoring

@app.route('/')
def dashboard():
    """Serve the main dashboard"""
    return render_template('dashboard.html')

# Enhanced dashboard data endpoint with better error handling
@app.route('/api/dashboard-data')
def api_dashboard_data():
    """Enhanced API endpoint with validation, caching, and fallback"""
    try:
        # Validate request parameters
        schema = DashboardQuerySchema()
        try:
            args = schema.load(request.args)
        except ValidationError as err:
            logger.warning("Invalid request parameters", errors=err.messages)
            return jsonify({'error': 'Invalid parameters', 'details': err.messages}), 400
        
        # Get dashboard data with automatic fallback
        data = get_dashboard_data(args['time_range'], args['environment'])
        
        # Always ensure we have the required structure for frontend
        if not data or 'stats' not in data:
            logger.warning("Dashboard data missing required fields, using mock data")
            data = get_mock_data()
        
        # Transform data for frontend with safe fallbacks
        transformed_data = {
            'topApis': data.get('top_apis_by_consumers', {'labels': [], 'data': []}),
            'ouUsage': data.get('ou_api_usage', {'labels': [], 'data': []}),
            'performanceTrends': data.get('performance_timechart', {'labels': [], 'datasets': []}),
            'dailyVolume': data.get('daily_app_usage', {'labels': [], 'datasets': []}),
            'errorDistribution': data.get('error_analysis', {'labels': [], 'data': []}),
            'stats': data.get('stats', {}),
            'metadata': data.get('metadata', {}),
            'timestamp': datetime.now().isoformat()
        }
        
        # Add status indicator for frontend
        if data.get('metadata', {}).get('data_source') == 'splunk':
            transformed_data['status'] = 'connected'
        else:
            transformed_data['status'] = 'mock_data'
        
        return jsonify(transformed_data)
        
    except Exception as e:
        logger.error("Error in dashboard data endpoint", error=str(e))
        # Return mock data instead of error to prevent frontend crashes
        mock_data = get_mock_data()
        return jsonify({
            'topApis': mock_data.get('top_apis_by_consumers', {}),
            'ouUsage': mock_data.get('ou_api_usage', {}),
            'performanceTrends': mock_data.get('performance_timechart', {}),
            'dailyVolume': mock_data.get('daily_app_usage', {}),
            'errorDistribution': mock_data.get('error_analysis', {}),
            'stats': mock_data.get('stats', {}),
            'metadata': mock_data.get('metadata', {}),
            'status': 'error_fallback',
            'timestamp': datetime.now().isoformat()
        })

@app.route('/api/splunk/search', methods=['POST'])
def direct_splunk_search():
    """Direct Splunk search endpoint using blocking HTTP API"""
    try:
        # Validate request
        request_data = request.get_json()
        if not request_data or 'search' not in request_data:
            return jsonify({'error': 'Missing search query in request body'}), 400
        
        search_query = request_data['search']
        earliest_time = request_data.get('earliest_time', '-30d@d')
        latest_time = request_data.get('latest_time', 'now')
        timeout = int(request_data.get('timeout', 30))
        
        logger.info("Direct Splunk search request", 
                   query_preview=search_query[:100],
                   earliest_time=earliest_time,
                   latest_time=latest_time)
        
        # Check if we have a Splunk connector
        if not splunk or not splunk.config.get('auth_token'):
            return jsonify({
                'error': 'Splunk not configured',
                'details': 'No authentication token available'
            }), 503
        
        # Prepare search URL and data
        search_url = f"{splunk.base_url}/services/search/jobs"
        
        search_data = {
            'search': search_query,
            'output_mode': 'json', 
            'exec_mode': 'blocking'
        }
        
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': f'Bearer {splunk.config["auth_token"]}'
        }
        
        # Make the request
        start_time = time.time()
        response = splunk.session.post(
            search_url,
            data=search_data,
            headers=headers,
            timeout=timeout + 5
        )
        
        response.raise_for_status()
        search_result = response.json()
        
        # Get the results
        if 'sid' not in search_result:
            return jsonify({'error': 'No SID returned from search'}), 500
        
        job_sid = search_result['sid']
        
        # Fetch results
        results_url = f"{splunk.base_url}/services/search/jobs/{job_sid}/results"
        results_params = {
            'output_mode': 'json',
            'count': 1000
        }
        
        results_response = splunk.session.get(
            results_url,
            params=results_params,
            headers=headers,
            timeout=15
        )
        
        results_response.raise_for_status()
        results_data = results_response.json()
        
        execution_time = time.time() - start_time
        
        # Return structured response
        return jsonify({
            'status': 'success',
            'sid': job_sid,
            'results': results_data.get('results', []),
            'execution_time': f"{execution_time:.2f}s",
            'result_count': len(results_data.get('results', [])),
            'metadata': {
                'earliest_time': earliest_time,
                'latest_time': latest_time,
                'search_query': search_query,
                'timestamp': datetime.now().isoformat()
            }
        })
        
    except requests.exceptions.HTTPError as e:
        error_msg = f"HTTP error: {e.response.status_code}"
        logger.error("Direct search HTTP error", error=str(e), status_code=e.response.status_code)
        
        if e.response.status_code == 401:
            return jsonify({'error': 'Authentication failed', 'details': error_msg}), 401
        
        return jsonify({'error': error_msg}), 500
        
    except requests.exceptions.Timeout:
        logger.error("Direct search timeout")
        return jsonify({'error': 'Search request timed out'}), 408
        
    except Exception as e:
        logger.error("Direct search failed", error=str(e))
        return jsonify({'error': 'Search request failed', 'details': str(e)}), 500

# Enhanced health check with better connection status
@app.route('/api/health')
def health_check():
    """Enhanced health check with connection details"""
    try:
        # Attempt connection test if we have a connector
        connection_success = False
        if splunk and splunk.config.get('auth_token'):
            connection_success = splunk.test_connection()
        
        splunk_status = "connected" if connection_success else "disconnected"
        session_status = splunk.get_session_status() if splunk else {}
        
        health_data = {
            'status': 'healthy' if connection_success else 'degraded',
            'timestamp': datetime.now().isoformat(),
            'services': {
                'splunk': {
                    'status': splunk_status,
                    'host': app.config['SPLUNK_HOST'] if splunk else 'not_configured',
                    'connection_attempts': session_status.get('connection_attempts', 0),
                    'session': session_status,
                    'auth_method': 'token',
                    'has_token': bool(app.config.get('SPLUNK_AUTH_TOKEN'))
                },
                'cache': {
                    'type': 'memory',
                    'status': 'connected',
                    'entries': len(memory_cache)
                }
            },
            'metrics': {
                'cache_entries': len(memory_cache),
                'cache_type': 'memory',
                'uptime_seconds': time.time(),
                'version': '2.0.0'
            },
            'configuration': {
                'cache_ttl': app.config['CACHE_TTL'],
                'environment': app.config.get('FLASK_ENV', 'production'),
                'debug': app.config['DEBUG']
            }
        }
        
        return jsonify(health_data)
        
    except Exception as e:
        logger.error("Health check failed", error=str(e))
        return jsonify({'error': 'Health check failed', 'status': 'unhealthy'}), 500

@app.route('/api/refresh-cache')
def refresh_cache():
    """Clear cache and refresh data"""
    try:
        cleared_count = clear_cache()
        
        return jsonify({
            'status': 'success',
            'message': 'Cache cleared successfully',
            'entries_cleared': cleared_count,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        logger.error("Cache refresh failed", error=str(e))
        return jsonify({'error': 'Cache refresh failed'}), 500

@app.route('/api/export/<format>')
def export_data(format):
    """Export dashboard data in different formats"""
    try:
        # Validate format
        if format not in ['json', 'csv', 'pdf']:
            return jsonify({'error': 'Invalid export format'}), 400
        
        # Get request parameters
        schema = DashboardQuerySchema()
        try:
            args = schema.load(request.args)
        except ValidationError as err:
            return jsonify({'error': 'Invalid parameters', 'details': err.messages}), 400
        
        # Get data
        data = get_dashboard_data(args['time_range'], args['environment'])
        
        if format == 'json':
            return jsonify(data)
        
        elif format == 'csv':
            # Convert data to CSV format
            output = io.StringIO()
            
            # Write KPI data
            output.write("Metric,Value\n")
            for key, value in data.get('stats', {}).items():
                output.write(f"{key},{value}\n")
            
            output.write("\nTop APIs by Consumers\n")
            output.write("API Name,Consumers\n")
            top_apis = data.get('top_apis_by_consumers', {})
            for api, count in zip(top_apis.get('labels', []), top_apis.get('data', [])):
                output.write(f"{api},{count}\n")
            
            output.write("\nOU Usage\n")
            output.write("Organization Unit,API Usage\n")
            ou_usage = data.get('ou_api_usage', {})
            for ou, count in zip(ou_usage.get('labels', []), ou_usage.get('data', [])):
                output.write(f"{ou},{count}\n")
            
            csv_content = output.getvalue()
            output.close()
            
            response = send_file(
                io.BytesIO(csv_content.encode()),
                mimetype='text/csv',
                as_attachment=True,
                download_name=f'icoe_dashboard_{datetime.now().strftime("%Y%m%d")}.csv'
            )
            return response
        
        elif format == 'pdf':
            # Generate PDF report
            buffer = io.BytesIO()
            doc = SimpleDocTemplate(buffer, pagesize=letter)
            styles = getSampleStyleSheet()
            elements = []
            
            # Title
            title = Paragraph("ICoE Integration Landscape Dashboard Report", styles['Title'])
            elements.append(title)
            elements.append(Spacer(1, 20))
            
            # Metadata
            metadata = data.get('metadata', {})
            elements.append(Paragraph(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", styles['Normal']))
            elements.append(Paragraph(f"Time Range: {metadata.get('time_range', 'N/A')}", styles['Normal']))
            elements.append(Paragraph(f"Environment: {metadata.get('environment', 'N/A')}", styles['Normal']))
            elements.append(Spacer(1, 20))
            
            # KPI Summary
            elements.append(Paragraph("Key Performance Indicators", styles['Heading1']))
            stats = data.get('stats', {})
            kpi_data = [['Metric', 'Value']]
            for key, value in stats.items():
                kpi_data.append([key.replace('_', ' ').title(), str(value)])
            
            kpi_table = Table(kpi_data)
            kpi_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 14),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black)
            ]))
            elements.append(kpi_table)
            
            # Build PDF
            doc.build(elements)
            buffer.seek(0)
            
            return send_file(
                buffer,
                mimetype='application/pdf',
                as_attachment=True,
                download_name=f'icoe_dashboard_{datetime.now().strftime("%Y%m%d")}.pdf'
            )
            
    except Exception as e:
        logger.error("Export failed", error=str(e), format=format)
        return jsonify({'error': f'Export failed: {str(e)}'}), 500

@app.route('/metrics')
def metrics():
    """Prometheus metrics endpoint"""
    return generate_latest(), 200, {'Content-Type': CONTENT_TYPE_LATEST}

# Background Tasks - Only start if needed
def background_cache_refresh():
    """Enhanced cache management for in-memory cache"""
    while True:
        try:
            time.sleep(app.config['CACHE_TTL'])
            logger.info("Starting background cache refresh")
            
            current_time = time.time()
            
            # Clean expired entries from memory cache
            expired_keys = [
                key for key, (_, timestamp) in memory_cache.items()
                if current_time - timestamp > app.config['CACHE_TTL']
            ]
            
            for key in expired_keys:
                del memory_cache[key]
            
            logger.info("Cache refresh completed", 
                      expired_keys=len(expired_keys),
                      active_keys=len(memory_cache))
            
        except Exception as e:
            logger.error("Background cache refresh failed", error=str(e))

def scheduled_health_check():
    """Enhanced health monitoring with less aggressive connection attempts"""
    try:
        # Update Prometheus metrics based on connection status
        if splunk and splunk.config.get('auth_token'):
            if splunk.test_connection():
                ACTIVE_CONNECTIONS.set(1)
            else:
                ACTIVE_CONNECTIONS.set(0)
        else:
            ACTIVE_CONNECTIONS.set(0)
            
    except Exception as e:
        logger.error("Scheduled health check failed", error=str(e))

# Enhanced Error Handlers
@app.errorhandler(400)
def bad_request(error):
    logger.warning("Bad request", error=str(error))
    return jsonify({'error': 'Bad request', 'message': str(error)}), 400

@app.errorhandler(404)
def not_found(error):
    logger.warning("Endpoint not found", path=request.path)
    return jsonify({'error': 'Endpoint not found'}), 404

@app.errorhandler(500)
def internal_error(error):
    logger.error("Internal server error", error=str(error))
    return jsonify({'error': 'Internal server error'}), 500

@app.errorhandler(Exception)
def handle_exception(e):
    logger.error("Unhandled exception", error=str(e), type=type(e).__name__)
    return jsonify({'error': 'An unexpected error occurred'}), 500

if __name__ == '__main__':
    # Create templates directory if it doesn't exist
    os.makedirs('templates', exist_ok=True)
    
    # Start background tasks in separate threads only if needed
    if not app.config['DEBUG']:
        # Start cache refresh thread
        cache_thread = threading.Thread(target=background_cache_refresh, daemon=True)
        cache_thread.start()
        
        # Start health check thread
        health_thread = threading.Thread(target=scheduled_health_check, daemon=True)
        health_thread.start()
        
        logger.info("Background tasks started")
    
    # Try to establish initial Splunk connection (non-blocking)
    if splunk and splunk.config.get('auth_token'):
        try:
            connection_thread = threading.Thread(target=splunk.connect, daemon=True)
            connection_thread.start()
            logger.info("Initial Splunk connection attempt started in background")
        except Exception as e:
            logger.warning("Failed to start initial connection thread", error=str(e))
    else:
        logger.warning("No Splunk token configured - dashboard will use mock data")
    
    # Start Flask app
    logger.info("Starting Flask application", host=app.config['HOST'], port=app.config['PORT'])
    app.run(
        host=app.config['HOST'], 
        port=app.config['PORT'], 
        debug=app.config['DEBUG']
    )
