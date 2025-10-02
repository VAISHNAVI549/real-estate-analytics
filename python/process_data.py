"""
Data Processing Module - Clean and load data into SQL database
"""
import logging
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from datetime import datetime
import hashlib
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


class DataCleaner:
    """Clean and validate real estate data"""
    
    def __init__(self):
        self.validation_rules = {
            'price': (0, 100_000_000),
            'sqft': (100, 50_000),
            'bedrooms': (0, 20),
            'bathrooms': (0, 15)
        }
    
    def clean_zillow_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean Zillow ZHVI data and convert to listings format"""
        logger.info(f"Cleaning Zillow data: {len(df)} records")
        
        if df.empty:
            return pd.DataFrame()
        
        try:
            # Zillow data is wide format (dates as columns), need to melt
            id_vars = ['RegionID', 'SizeRank', 'RegionName', 'RegionType', 'StateName']
            id_vars = [col for col in id_vars if col in df.columns]
            
            # Get date columns
            date_cols = [col for col in df.columns if col not in id_vars]
            
            # Melt to long format
            df_long = df.melt(
                id_vars=id_vars,
                value_vars=date_cols,
                var_name='date',
                value_name='price'
            )
            
            # Clean and standardize
            df_clean = df_long.copy()
            df_clean['date'] = pd.to_datetime(df_clean['date'])
            df_clean['price'] = pd.to_numeric(df_clean['price'], errors='coerce')
            
            # Remove null prices
            df_clean = df_clean.dropna(subset=['price'])
            
            # Create listing_id
            df_clean['listing_id'] = df_clean.apply(
                lambda x: hashlib.md5(f"zillow_{x['RegionID']}_{x['date']}".encode()).hexdigest()[:16],
                axis=1
            )
            
            # Standardize columns
            df_clean = df_clean.rename(columns={
                'RegionName': 'city',
                'StateName': 'region'
            })
            
            # Add default values
            df_clean['property_type'] = 'condo'
            df_clean['sale_type'] = 'sale'
            df_clean['ownership'] = 'unknown'
            df_clean['bedrooms'] = None
            df_clean['bathrooms'] = None
            df_clean['sqft'] = None
            df_clean['tax'] = None
            
            # Select and order columns
            columns = ['listing_id', 'date', 'region', 'city', 'property_type', 
                      'price', 'tax', 'sale_type', 'ownership', 'bedrooms', 
                      'bathrooms', 'sqft']
            
            df_clean = df_clean[[col for col in columns if col in df_clean.columns]]
            
            logger.info(f"Cleaned Zillow data: {len(df_clean)} records")
            return df_clean
            
        except Exception as e:
            logger.error(f"Error cleaning Zillow data: {e}")
            return pd.DataFrame()
    
    def clean_redfin_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean Redfin market data"""
        logger.info(f"Cleaning Redfin data: {len(df)} records")
        
        if df.empty:
            return pd.DataFrame()
        
        try:
            df_clean = df.copy()
            
            # Convert date
            df_clean['period_end'] = pd.to_datetime(df_clean['period_end'])
            
            # Create listing_id
            df_clean['listing_id'] = df_clean.apply(
                lambda x: hashlib.md5(
                    f"redfin_{x.get('region', '')}_{x['period_end']}".encode()
                ).hexdigest()[:16],
                axis=1
            )
            
            # Standardize columns
            rename_map = {
                'period_end': 'date',
                'region': 'city',
                'state_code': 'region',
                'median_sale_price': 'price',
                'property_type': 'property_type'
            }
            
            df_clean = df_clean.rename(columns=rename_map)
            
            # Add defaults
            df_clean['sale_type'] = 'sale'
            df_clean['ownership'] = 'unknown'
            df_clean['tax'] = None
            df_clean['bedrooms'] = None
            df_clean['bathrooms'] = None
            df_clean['sqft'] = None
            
            # Handle property type
            if 'property_type' in df_clean.columns:
                df_clean['property_type'] = df_clean['property_type'].str.lower()
                df_clean['property_type'] = df_clean['property_type'].replace({
                    'single family residential': 'independent',
                    'condo/co-op': 'condo',
                    'townhouse': 'townhouse'
                })
            else:
                df_clean['property_type'] = 'other'
            
            # Select columns
            columns = ['listing_id', 'date', 'region', 'city', 'property_type',
                      'price', 'tax', 'sale_type', 'ownership', 'bedrooms',
                      'bathrooms', 'sqft']
            
            df_clean = df_clean[[col for col in columns if col in df_clean.columns]]
            
            # Remove duplicates
            df_clean = df_clean.drop_duplicates(subset=['listing_id'])
            
            logger.info(f"Cleaned Redfin data: {len(df_clean)} records")
            return df_clean
            
        except Exception as e:
            logger.error(f"Error cleaning Redfin data: {e}")
            return pd.DataFrame()
    
    def validate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate data against business rules"""
        logger.info(f"Validating data: {len(df)} records")
        
        initial_count = len(df)
        df_valid = df.copy()
        
        # Validate price
        if 'price' in df_valid.columns:
            min_price, max_price = self.validation_rules['price']
            df_valid = df_valid[
                (df_valid['price'] >= min_price) & 
                (df_valid['price'] <= max_price)
            ]
        
        # Validate sqft
        if 'sqft' in df_valid.columns:
            min_sqft, max_sqft = self.validation_rules['sqft']
            df_valid.loc[
                (df_valid['sqft'] < min_sqft) | 
                (df_valid['sqft'] > max_sqft),
                'sqft'
            ] = None
        
        # Validate bedrooms
        if 'bedrooms' in df_valid.columns:
            min_bed, max_bed = self.validation_rules['bedrooms']
            df_valid.loc[
                (df_valid['bedrooms'] < min_bed) | 
                (df_valid['bedrooms'] > max_bed),
                'bedrooms'
            ] = None
        
        # Validate bathrooms
        if 'bathrooms' in df_valid.columns:
            min_bath, max_bath = self.validation_rules['bathrooms']
            df_valid.loc[
                (df_valid['bathrooms'] < min_bath) | 
                (df_valid['bathrooms'] > max_bath),
                'bathrooms'
            ] = None
        
        removed = initial_count - len(df_valid)
        if removed > 0:
            logger.warning(f"Removed {removed} invalid records")
        
        logger.info(f"Validation complete: {len(df_valid)} valid records")
        return df_valid


class DataLoader:
    """Load cleaned data into PostgreSQL database"""
    
    def __init__(self):
        self.engine = create_engine(config.DB_CONNECTION_STRING)
        logger.info("Database connection established")
    
    def load_listings(self, df: pd.DataFrame) -> int:
        """Load listings data into database"""
        if df.empty:
            logger.warning("No listings data to load")
            return 0
        
        logger.info(f"Loading {len(df)} listings into database")
        
        records_loaded = 0
        records_failed = 0
        
        for idx, row in df.iterrows():
            try:
                # Use individual transaction for each record
                with self.engine.begin() as conn:
                    query = text("""
                        INSERT INTO listings 
                        (listing_id, date, region, city, property_type, price, 
                         tax, sale_type, ownership, bedrooms, bathrooms, sqft)
                        VALUES 
                        (:listing_id, :date, :region, :city, :property_type, :price,
                         :tax, :sale_type, :ownership, :bedrooms, :bathrooms, :sqft)
                        ON CONFLICT (listing_id) 
                        DO UPDATE SET 
                            price = EXCLUDED.price,
                            updated_at = CURRENT_TIMESTAMP
                    """)
                    
                    conn.execute(query, {
                        'listing_id': row['listing_id'],
                        'date': row['date'],
                        'region': row['region'],
                        'city': row.get('city'),
                        'property_type': row.get('property_type'),
                        'price': float(row['price']) if pd.notna(row['price']) else None,
                        'tax': float(row['tax']) if pd.notna(row.get('tax')) else None,
                        'sale_type': row.get('sale_type'),
                        'ownership': row.get('ownership'),
                        'bedrooms': int(row['bedrooms']) if pd.notna(row.get('bedrooms')) else None,
                        'bathrooms': float(row['bathrooms']) if pd.notna(row.get('bathrooms')) else None,
                        'sqft': int(row['sqft']) if pd.notna(row.get('sqft')) else None
                    })
                    records_loaded += 1
                    
                    # Progress indicator every 100 records
                    if records_loaded % 100 == 0:
                        logger.info(f"  Progress: {records_loaded} records loaded...")
                    
            except Exception as e:
                records_failed += 1
                if records_failed <= 3:  # Only log first 3 errors
                    logger.warning(f"Skipping record {row.get('listing_id', 'unknown')}: {str(e)[:100]}")
                continue
        
        if records_failed > 3:
            logger.warning(f"... and {records_failed - 3} more records failed")
        
        logger.info(f"Successfully loaded {records_loaded} listings ({records_failed} failed)")
        return records_loaded
    
    def load_macro_indicators(self, df: pd.DataFrame, region: str) -> int:
        """Load macroeconomic indicators"""
        if df.empty:
            logger.warning("No macro data to load")
            return 0
        
        logger.info(f"Loading {len(df)} macro indicators for {region}")
        
        records_loaded = 0
        records_failed = 0
        
        for _, row in df.iterrows():
            try:
                # Use individual transaction for each record
                with self.engine.begin() as conn:
                    query = text("""
                        INSERT INTO macro_indicators 
                        (region, date, mortgage_rate)
                        VALUES (:region, :date, :mortgage_rate)
                        ON CONFLICT (region, date)
                        DO UPDATE SET 
                            mortgage_rate = EXCLUDED.mortgage_rate,
                            updated_at = CURRENT_TIMESTAMP
                    """)
                    
                    conn.execute(query, {
                        'region': region,
                        'date': row['date'],
                        'mortgage_rate': float(row['value']) if pd.notna(row['value']) else None
                    })
                    records_loaded += 1
                    
            except Exception as e:
                records_failed += 1
                if records_failed <= 3:  # Only log first 3 errors
                    logger.warning(f"Skipping invalid record (date: {row.get('date', 'unknown')}): {str(e)[:100]}")
                continue
        
        if records_failed > 3:
            logger.warning(f"... and {records_failed - 3} more records failed")
        
        logger.info(f"Successfully loaded {records_loaded} macro indicators ({records_failed} failed)")
        return records_loaded
    
    def close(self):
        """Close database connection"""
        self.engine.dispose()
        logger.info("Database connection closed")


def main():
    """Main ETL pipeline execution"""
    logger.info("=" * 80)
    logger.info("REAL ESTATE DATA PROCESSOR - Starting ETL Pipeline")
    logger.info("=" * 80)
    
    # Initialize components
    cleaner = DataCleaner()
    loader = DataLoader()
    
    try:
        # Load raw data files
        logger.info("Loading raw data files...")
        
        # Find latest files
        import glob
        zillow_files = glob.glob(str(config.RAW_DATA_DIR / 'zillow_raw_*.csv'))
        redfin_files = glob.glob(str(config.RAW_DATA_DIR / 'redfin_raw_*.csv'))
        fred_files = glob.glob(str(config.RAW_DATA_DIR / 'fred_MORTGAGE30US_*.csv'))
        
        total_loaded = 0
        
        # Process Zillow data
        if zillow_files:
            latest_zillow = max(zillow_files)
            logger.info(f"Processing {latest_zillow}")
            df_zillow = pd.read_csv(latest_zillow)
            df_zillow_clean = cleaner.clean_zillow_data(df_zillow)
            df_zillow_valid = cleaner.validate_data(df_zillow_clean)
            loaded = loader.load_listings(df_zillow_valid)
            total_loaded += loaded
        else:
            logger.warning("No Zillow files found")
        
        # Process Redfin data
        if redfin_files:
            latest_redfin = max(redfin_files)
            logger.info(f"Processing {latest_redfin}")
            df_redfin = pd.read_csv(latest_redfin)
            df_redfin_clean = cleaner.clean_redfin_data(df_redfin)
            df_redfin_valid = cleaner.validate_data(df_redfin_clean)
            loaded = loader.load_listings(df_redfin_valid)
            total_loaded += loaded
        else:
            logger.warning("No Redfin files found")
        
        # Process FRED mortgage data
        if fred_files:
            latest_fred = max(fred_files)
            logger.info(f"Processing {latest_fred}")
            df_fred = pd.read_csv(latest_fred)
            loaded = loader.load_macro_indicators(df_fred, 'florida')
            total_loaded += loaded
        else:
            logger.warning("No FRED files found")
        
        # Summary
        print("\n" + "=" * 80)
        print("ETL PIPELINE SUMMARY")
        print("=" * 80)
        print(f"Total records loaded: {total_loaded}")
        print(f"Files processed: {len(zillow_files) + len(redfin_files) + len(fred_files)}")
        
        logger.info("ETL Pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"ETL Pipeline failed: {e}")
        raise
    
    finally:
        loader.close()


if __name__ == "__main__":
    main()
