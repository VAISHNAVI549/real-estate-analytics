"""
Configuration module for Real Estate Analytics Application
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Project paths
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / 'data'
RAW_DATA_DIR = DATA_DIR / 'raw'
PROCESSED_DATA_DIR = DATA_DIR / 'processed'
OUTPUT_DIR = BASE_DIR / 'output'
CHARTS_DIR = OUTPUT_DIR / 'charts'
FORECASTS_DIR = OUTPUT_DIR / 'forecasts'
LOGS_DIR = BASE_DIR / 'logs'

# Create directories if they don't exist
for directory in [RAW_DATA_DIR, PROCESSED_DATA_DIR, CHARTS_DIR, FORECASTS_DIR, LOGS_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'real_estate_db'),
    'user': os.getenv('DB_USER', os.getenv('USER', 'postgres')),
    'password': os.getenv('DB_PASSWORD', '')
}

# Database connection string
DB_CONNECTION_STRING = (
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
    f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

# API Keys (add your own)
API_KEYS = {
    'zillow': os.getenv('ZILLOW_API_KEY', ''),
    'census': os.getenv('CENSUS_API_KEY', ''),
    'fred': os.getenv('FRED_API_KEY', ''),  # Federal Reserve Economic Data
}

# Target region configuration
TARGET_REGIONS = {
    'florida': {
        'state': 'FL',
        'cities': ['Miami', 'Tampa', 'Orlando', 'Jacksonville', 'Fort Lauderdale'],
        'census_fips': '12'  # Florida FIPS code
    },
    'california': {
        'state': 'CA',
        'cities': ['Los Angeles', 'San Francisco', 'San Diego', 'San Jose', 'Sacramento'],
        'census_fips': '06'
    }
}

# Default region
DEFAULT_REGION = 'florida'

# Data sources
DATA_SOURCES = {
    'zillow': 'https://www.zillow.com/research/data/',
    'census': 'https://api.census.gov/data',
    'fred': 'https://api.stlouisfed.org/fred/series',
    'redfin': 'https://redfin-public-data.s3.us-west-2.amazonaws.com',
}

# Forecasting parameters
FORECAST_CONFIG = {
    'horizon': 24,  # months to forecast
    'confidence_level': 0.95,
    'test_size': 0.2,  # for train-test split
    'models': ['ARIMA', 'SARIMA', 'Prophet']
}

# Logging configuration
LOG_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'file': LOGS_DIR / 'app.log'
}
