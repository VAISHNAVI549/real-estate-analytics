"""
Data Fetcher Module - Fetches real estate data from various sources
"""
import logging
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime
import time
from typing import Dict, List, Optional
import config

# Setup logging
logging.basicConfig(
    level=config.LOG_CONFIG['level'],
    format=config.LOG_CONFIG['format'],
    handlers=[
        logging.FileHandler(config.LOG_CONFIG['file']),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class DataFetcher:
    """Base class for fetching real estate data"""
    
    def __init__(self, region: str = config.DEFAULT_REGION):
        self.region = region
        self.region_config = config.TARGET_REGIONS.get(region, {})
        self.api_keys = config.API_KEYS
        
    def fetch_zillow_data(self) -> pd.DataFrame:
        """
        Fetch Zillow research data
        Zillow provides public datasets at: https://www.zillow.com/research/data/
        """
        logger.info(f"Fetching Zillow data for {self.region}")
        
        try:
            # Zillow Research Data - Home Values (ZHVI)
            url = "https://files.zillowstatic.com/research/public_csvs/zhvi/Metro_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"
            
            df = pd.read_csv(url)
            logger.info(f"Successfully fetched Zillow data: {len(df)} records")
            
            # Save raw data
            output_path = config.RAW_DATA_DIR / f'zillow_raw_{datetime.now().strftime("%Y%m%d")}.csv'
            df.to_csv(output_path, index=False)
            logger.info(f"Saved raw data to {output_path}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error fetching Zillow data: {e}")
            return pd.DataFrame()
    
    def fetch_redfin_data(self) -> pd.DataFrame:
        """
        Fetch Redfin market data
        Redfin provides public datasets on S3
        """
        logger.info(f"Fetching Redfin data for {self.region}")
        
        try:
            # Redfin Weekly Market Data
            url = "https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/state_market_tracker.tsv000.gz"
            
            df = pd.read_csv(url, compression='gzip', sep='\t')
            
            # Filter for target state
            state_code = self.region_config.get('state', '')
            if state_code:
                df = df[df['state_code'] == state_code]
            
            logger.info(f"Successfully fetched Redfin data: {len(df)} records")
            
            # Save raw data
            output_path = config.RAW_DATA_DIR / f'redfin_raw_{datetime.now().strftime("%Y%m%d")}.csv'
            df.to_csv(output_path, index=False)
            
            return df
            
        except Exception as e:
            logger.error(f"Error fetching Redfin data: {e}")
            return pd.DataFrame()
    
    def fetch_census_data(self, year: int = 2020) -> pd.DataFrame:
        """
        Fetch US Census data
        Requires API key from: https://api.census.gov/data/key_signup.html
        """
        logger.info(f"Fetching Census data for {self.region}, year {year}")
        
        if not self.api_keys.get('census'):
            logger.warning("Census API key not found. Using sample data.")
            return self._generate_sample_census_data()
        
        try:
            fips_code = self.region_config.get('census_fips', '')
            api_key = self.api_keys['census']
            
            # ACS 5-Year Data - Housing characteristics
            url = f"https://api.census.gov/data/{year}/acs/acs5"
            
            params = {
                'get': 'B25001_001E,B25002_001E,B25003_001E,B25003_002E,B25003_003E',  # Housing units, occupancy, tenure
                'for': f'state:{fips_code}',
                'key': api_key
            }
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            df = pd.DataFrame(data[1:], columns=data[0])
            
            logger.info(f"Successfully fetched Census data: {len(df)} records")
            
            # Save raw data
            output_path = config.RAW_DATA_DIR / f'census_raw_{year}_{datetime.now().strftime("%Y%m%d")}.csv'
            df.to_csv(output_path, index=False)
            
            return df
            
        except Exception as e:
            logger.error(f"Error fetching Census data: {e}")
            return self._generate_sample_census_data()
    
    def fetch_fred_data(self, series_id: str = 'MORTGAGE30US') -> pd.DataFrame:
        """
        Fetch Federal Reserve Economic Data (FRED)
        Requires API key from: https://fred.stlouisfed.org/docs/api/api_key.html
        """
        logger.info(f"Fetching FRED data for series {series_id}")
        
        if not self.api_keys.get('fred'):
            logger.warning("FRED API key not found. Using sample data.")
            return self._generate_sample_fred_data()
        
        try:
            url = "https://api.stlouisfed.org/fred/series/observations"
            
            params = {
                'series_id': series_id,
                'api_key': self.api_keys['fred'],
                'file_type': 'json',
                'observation_start': '2000-01-01'
            }
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            df = pd.DataFrame(data['observations'])
            
            logger.info(f"Successfully fetched FRED data: {len(df)} records")
            
            # Save raw data
            output_path = config.RAW_DATA_DIR / f'fred_{series_id}_{datetime.now().strftime("%Y%m%d")}.csv'
            df.to_csv(output_path, index=False)
            
            return df
            
        except Exception as e:
            logger.error(f"Error fetching FRED data: {e}")
            return self._generate_sample_fred_data()
    
    def _generate_sample_census_data(self) -> pd.DataFrame:
        """Generate sample census data for demonstration"""
        logger.info("Generating sample Census data")
        
        years = range(2000, 2024)
        data = []
        
        for year in years:
            data.append({
                'year': year,
                'region': self.region,
                'total_housing_units': 8000000 + (year - 2000) * 50000,
                'owner_occupied': 5500000 + (year - 2000) * 30000,
                'renter_occupied': 2500000 + (year - 2000) * 20000
            })
        
        return pd.DataFrame(data)
    
    def _generate_sample_fred_data(self) -> pd.DataFrame:
        """Generate sample FRED data for demonstration"""
        logger.info("Generating sample FRED data")
        
        dates = pd.date_range(start='2000-01-01', end='2023-12-31', freq='M')
        data = []
        
        base_rate = 6.5
        for date in dates:
            # Simulate mortgage rate fluctuations
            rate = base_rate + (date.year - 2000) * 0.1 - ((date.year - 2010) ** 2) * 0.02
            data.append({
                'date': date.strftime('%Y-%m-%d'),
                'value': max(2.5, min(8.0, rate))
            })
        
        return pd.DataFrame(data)
    
    def fetch_all_data(self) -> Dict[str, pd.DataFrame]:
        """Fetch all available data sources"""
        logger.info("Starting comprehensive data fetch")
        
        datasets = {
            'zillow': self.fetch_zillow_data(),
            'redfin': self.fetch_redfin_data(),
            'census': self.fetch_census_data(),
            'fred_mortgage': self.fetch_fred_data('MORTGAGE30US'),
        }
        
        logger.info("Data fetch complete")
        return datasets


def main():
    """Main execution function"""
    logger.info("=" * 80)
    logger.info("REAL ESTATE DATA FETCHER - Starting")
    logger.info("=" * 80)
    
    # Initialize fetcher
    fetcher = DataFetcher(region='florida')
    
    # Fetch all data
    datasets = fetcher.fetch_all_data()
    
    # Display summary
    print("\n" + "=" * 80)
    print("DATA FETCH SUMMARY")
    print("=" * 80)
    for name, df in datasets.items():
        print(f"\n{name.upper()}:")
        print(f"  Records: {len(df)}")
        print(f"  Columns: {', '.join(df.columns[:5])}{'...' if len(df.columns) > 5 else ''}")
    
    logger.info("Data fetch completed successfully")


if __name__ == "__main__":
    main()
