import os
import time
import io
from datetime import datetime
from typing import Dict, Any

# Flask and extensions
from flask import Flask, render_template, jsonify, request, send_file
from flask_cors import CORS
from werkzeug.exceptions import BadRequest, NotFound, InternalServerError
from marshmallow import ValidationError

# Prometheus monitoring
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

# Structured logging
import structlog

# Local imports
from config import config
from models import DashboardQuerySchema, CustomQuerySchema, ExportQuerySchema
from queries import SPLUNK_QUERIES, get_query_by_name, prepare_query
from cache import cache_result, clear_cache, get_cache_stats
from splunk_connector import SplunkConnector
from data_processing import (
    get_mock_data, 
    process_splunk_data, 
    calculate_summary_stats,
    transform_data_for_frontend
)
from tasks import create_background_task_manager
from utils import (
    create_directories, 
    export_to_csv, 
    export_to_pdf,
    validate_time_range,
    validate_environment,
    sanitize_query,
    measure_execution_time,
    retry_on_failure
)

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

def has_valid_auth_config(splunk_connector):
    """Check if we have valid authentication configuration for any auth method"""
    if not splunk_connector:
        return False
    
    auth_method = splunk_connector.config.get('auth_method', 'username_password')
    
    if auth_method == 'username_password':
        return bool(splunk_connector.config.get('username') and splunk_connector.config.get('password'))
    elif auth_method == 'bearer_token':
        return bool(splunk_connector.config.get('bearer_token'))
    
    return False

def create_app(config_name='default'):
    """Application factory"""
    # Create Flask app
    app = Flask(__name__)
    
    # Load configuration
    app.config.from_object(config[config_name])
    CORS(app)
    
    # Create directories
    create_directories()
    
    # Initialize Splunk connector
    try:
        splunk = SplunkConnector(app.config)
        logger.info("SplunkConnector initialized")
    except Exception as e:
        logger.error("Failed to initialize SplunkConnector", error=str(e))
        splunk = None
    
    # Initialize background task manager
    task_manager = create_background_task_manager(app.config, splunk)
    
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

    # Enhanced dashboard data function with better fallback
    @cache_result('dashboard_data', ttl=300, use_mock_on_failure=True)
    @measure_execution_time
    @retry_on_failure(max_retries=2)
    def get_dashboard_data(time_range: str = '30d', environment: str = 'prod') -> Dict[str, Any]:
        """Enhanced dashboard data fetching with automatic fallback"""
        try:
            dashboard_data = {}
            earliest_time = f"-{time_range}@d"
            
            # Check if we have a Splunk connector with valid auth
            if not has_valid_auth_config(splunk):
                logger.warning("No valid Splunk authentication available, using mock data")
                return get_mock_data()
            
            # Test connection before proceeding
            if not splunk.ensure_valid_session():
                logger.warning("Cannot establish Splunk session, using mock data")
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
                    query = get_query_by_name(query_name)
                    prepared_query = prepare_query(query, environment)
                    
                    logger.info("Executing query", query_name=query_name, environment=environment, time_range=time_range)
                    
                    try:
                        result = splunk.execute_query(prepared_query, earliest_time, timeout=20)
                        processed_result = process_splunk_data(result, query_name)
                        
                        # Only add result if it has actual data
                        if processed_result and (processed_result.get('labels') or processed_result.get('datasets')):
                            dashboard_data[query_name] = processed_result
                            successful_queries += 1
                            SPLUNK_QUERY_COUNT.labels(query_type=query_name, status='success').inc()
                        else:
                            logger.warning("Query returned no useful data", query_name=query_name)
                            SPLUNK_QUERY_COUNT.labels(query_type=query_name, status='no_data').inc()
                            
                    except Exception as query_error:
                        logger.error("Query execution failed", query_name=query_name, error=str(query_error))
                        SPLUNK_QUERY_COUNT.labels(query_type=query_name, status='failed').inc()
            
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
            SPLUNK_QUERY_COUNT.labels(query_type='dashboard', status='error').inc()
            return get_mock_data()

    # Routes
    @app.route('/')
    def dashboard():
        """Serve the main dashboard"""
        return render_template('dashboard.html')

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
            
            # Validate individual parameters
            if not validate_time_range(args['time_range']):
                return jsonify({'error': 'Invalid time range'}), 400
                
            if not validate_environment(args['environment']):
                return jsonify({'error': 'Invalid environment'}), 400
            
            # Get dashboard data with automatic fallback
            data = get_dashboard_data(args['time_range'], args['environment'])
            
            # Always ensure we have the required structure for frontend
            if not data or 'stats' not in data:
                logger.warning("Dashboard data missing required fields, using mock data")
                data = get_mock_data()
            
            # Transform data for frontend
            transformed_data = transform_data_for_frontend(data)
            
            return jsonify(transformed_data)
            
        except Exception as e:
            logger.error("Error in dashboard data endpoint", error=str(e))
            # Return mock data instead of error to prevent frontend crashes
            mock_data = get_mock_data()
            transformed_data = transform_data_for_frontend(mock_data)
            transformed_data['status'] = 'error_fallback'
            return jsonify(transformed_data)

    @app.route('/api/splunk/search', methods=['POST'])
    def direct_splunk_search():
        """Direct Splunk search endpoint using blocking HTTP API"""
        try:
            # Validate request
            schema = CustomQuerySchema()
            try:
                request_data = schema.load(request.get_json() or {})
            except ValidationError as err:
                return jsonify({'error': 'Invalid request data', 'details': err.messages}), 400
            
            query = sanitize_query(request_data['query'])
            earliest_time = request_data.get('earliest_time', '-30d@d')
            latest_time = request_data.get('latest_time', 'now')
            timeout = request_data.get('timeout', 30)
            
            logger.info("Direct Splunk search request", 
                       query_preview=query[:100],
                       earliest_time=earliest_time,
                       latest_time=latest_time)
            
            # Check if we have valid Splunk authentication
            if not has_valid_auth_config(splunk):
                return jsonify({
                    'error': 'Splunk not configured',
                    'details': 'No valid authentication credentials available'
                }), 503
            
            # Test connection
            if not splunk.ensure_valid_session():
                return jsonify({
                    'error': 'Splunk connection failed',
                    'details': 'Unable to establish session with Splunk'
                }), 503
            
            # Execute the search
            start_time = time.time()
            results = splunk.execute_query(query, earliest_time, latest_time, timeout)
            execution_time = time.time() - start_time
            
            # Return structured response
            return jsonify({
                'status': 'success',
                'results': results,
                'execution_time': f"{execution_time:.2f}s",
                'result_count': len(results),
                'metadata': {
                    'earliest_time': earliest_time,
                    'latest_time': latest_time,
                    'search_query': query,
                    'timestamp': datetime.now().isoformat(),
                    'auth_method': splunk.config.get('auth_method', 'unknown')
                }
            })
            
        except ValueError as e:
            logger.warning("Invalid query submitted", error=str(e))
            return jsonify({'error': 'Invalid query', 'details': str(e)}), 400
            
        except Exception as e:
            logger.error("Direct search failed", error=str(e))
            return jsonify({'error': 'Search request failed', 'details': str(e)}), 500

    @app.route('/api/health')
    def health_check():
        """Enhanced health check with connection details for all auth methods"""
        try:
            # Attempt connection test if we have valid auth
            connection_success = False
            if has_valid_auth_config(splunk):
                connection_success = splunk.test_connection()
            
            splunk_status = "connected" if connection_success else "disconnected"
            session_status = splunk.get_status() if splunk else {}
            
            # Simple cache stats fallback
            try:
                cache_stats = get_cache_stats()
            except:
                cache_stats = {'total_entries': 0, 'hits': 0, 'misses': 0}
            
            # Determine credential status based on auth method
            auth_method = session_status.get('auth_method', 'unknown')
            has_credentials = False
            credential_details = {}
            
            if auth_method == 'username_password':
                has_credentials = bool(app.config.get('SPLUNK_USERNAME') and app.config.get('SPLUNK_PASSWORD'))
                credential_details = {
                    'username': app.config.get('SPLUNK_USERNAME'),
                    'has_password': bool(app.config.get('SPLUNK_PASSWORD'))
                }
            elif auth_method == 'bearer_token':
                has_credentials = bool(app.config.get('SPLUNK_BEARER_TOKEN'))
                credential_details = {
                    'has_bearer_token': bool(app.config.get('SPLUNK_BEARER_TOKEN')),
                    'token_type': app.config.get('SPLUNK_TOKEN_TYPE', 'Splunk')
                }
            
            health_data = {
                'status': 'healthy' if connection_success else 'degraded',
                'timestamp': datetime.now().isoformat(),
                'services': {
                    'splunk': {
                        'status': splunk_status,
                        'host': app.config.get('SPLUNK_HOST', 'not_configured'),
                        'connection_attempts': session_status.get('connection_attempts', 0),
                        'session': session_status,
                        'auth_method': auth_method,
                        'has_credentials': has_credentials,
                        'credential_details': credential_details
                    },
                    'cache': {
                        'type': 'memory',
                        'status': 'connected',
                        'stats': cache_stats
                    }
                },
                'metrics': {
                    'cache_entries': cache_stats.get('total_entries', 0),
                    'cache_type': 'memory',
                    'uptime_seconds': time.time(),
                    'version': '2.0.0'
                },
                'configuration': {
                    'cache_ttl': app.config.get('CACHE_TTL', 300),
                    'environment': app.config.get('FLASK_ENV', 'development'),
                    'debug': app.config.get('DEBUG', False)
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
            # Validate format and parameters
            if format not in ['json', 'csv', 'pdf']:
                return jsonify({'error': 'Invalid export format'}), 400
            
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
                csv_output = export_to_csv(data)
                csv_content = csv_output.getvalue()
                csv_output.close()
                
                return send_file(
                    io.BytesIO(csv_content.encode()),
                    mimetype='text/csv',
                    as_attachment=True,
                    download_name=f'icoe_dashboard_{datetime.now().strftime("%Y%m%d")}.csv'
                )
            
            elif format == 'pdf':
                pdf_buffer = export_to_pdf(data)
                
                return send_file(
                    pdf_buffer,
                    mimetype='application/pdf',
                    as_attachment=True,
                    download_name=f'icoe_dashboard_{datetime.now().strftime("%Y%m%d")}.pdf'
                )
                
        except Exception as e:
            logger.error("Export failed", error=str(e), format=format)
            return jsonify({'error': f'Export failed: {str(e)}'}), 500

    @app.route('/api/test-query/<query_name>')
    def test_single_query(query_name):
        """Test a single query with proper auth validation"""
        try:
            if query_name not in SPLUNK_QUERIES:
                return jsonify({'error': 'Query not found'}), 404
            
            # Check authentication
            if not has_valid_auth_config(splunk):
                return jsonify({
                    'error': 'Splunk not configured',
                    'details': 'No valid authentication available'
                }), 503
            
            # Test connection
            if not splunk.ensure_valid_session():
                return jsonify({
                    'error': 'Connection failed',
                    'details': 'Unable to establish Splunk session'
                }), 503
            
            query = prepare_query(SPLUNK_QUERIES[query_name], 'prod') + " | head 5"
            results = splunk.execute_query(query, "-7d@d", "now", 30)
            
            return jsonify({
                'query_name': query_name,
                'count': len(results),
                'results': results[:3],
                'auth_method': splunk.config.get('auth_method', 'unknown'),
                'status': 'success'
            })
            
        except Exception as e:
            logger.error(f"Test query {query_name} failed", error=str(e))
            return jsonify({
                'error': 'Query test failed',
                'details': str(e),
                'query_name': query_name
            }), 500

    @app.route('/metrics')
    def metrics():
        """Prometheus metrics endpoint"""
        return generate_latest(), 200, {'Content-Type': CONTENT_TYPE_LATEST}

    # Error Handlers
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

    # Store references for cleanup
    app.splunk = splunk
    app.task_manager = task_manager
    
    return app

def setup_background_tasks(app):
    """Setup background tasks for the application"""
    try:
        if not app.config.get('DEBUG', False):
            app.task_manager.start_background_tasks()
            logger.info("Background tasks started")
        else:
            logger.info("Background tasks disabled in debug mode")
            
        # Try to establish Splunk connection in background
        if has_valid_auth_config(app.splunk):
            def background_connect():
                try:
                    time.sleep(2)  # Give Flask time to start
                    success = app.splunk.connect()
                    auth_method = app.splunk.config.get('auth_method', 'unknown')
                    if success:
                        logger.info("Background Splunk connection established", auth_method=auth_method)
                    else:
                        logger.info("Background Splunk connection failed - will retry on requests", auth_method=auth_method)
                except Exception as e:
                    logger.info("Background Splunk connection error", error=str(e))
            
            import threading
            connection_thread = threading.Thread(target=background_connect, daemon=True)
            connection_thread.start()
        else:
            logger.warning("No valid Splunk authentication configured - dashboard will use mock data")
            
    except Exception as e:
        logger.error("Failed to setup background tasks", error=str(e))

if __name__ == '__main__':
    # Get environment
    env = os.getenv('FLASK_ENV', 'development')
    
    # Create app
    app = create_app(env)
    
    # Setup background tasks
    setup_background_tasks(app)
    
    # Start Flask app
    logger.info("Starting Flask application", 
               host=app.config['HOST'], 
               port=app.config['PORT'],
               environment=env)
    
    try:
        app.run(
            host=app.config['HOST'], 
            port=app.config['PORT'], 
            debug=app.config['DEBUG']
        )
    except KeyboardInterrupt:
        logger.info("Application stopped by user")
        if hasattr(app, 'task_manager'):
            app.task_manager.stop_background_tasks()
    except Exception as e:
        logger.error("Application failed to start", error=str(e))
        raise
