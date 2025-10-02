"""
Analytics Module - Perform SQL and Python-based analytics
"""
import logging
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
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

# Set visualization style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)


class RealEstateAnalytics:
    """Perform comprehensive real estate analytics"""
    
    def __init__(self):
        self.engine = create_engine(config.DB_CONNECTION_STRING)
        logger.info("Analytics engine initialized")
    
    def get_rent_vs_own_distribution(self, region: str = None) -> pd.DataFrame:
        """Query: Rent vs Own distribution by region"""
        logger.info("Analyzing rent vs own distribution")
        
        query = """
            SELECT 
                region,
                EXTRACT(YEAR FROM date) as year,
                sale_type,
                COUNT(*) as count,
                AVG(price) as avg_price,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) as median_price
            FROM listings
            WHERE 1=1
        """
        
        if region:
            query += f" AND region = '{region}'"
        
        query += """
            GROUP BY region, EXTRACT(YEAR FROM date), sale_type
            ORDER BY region, year, sale_type
        """
        
        df = pd.read_sql(query, self.engine)
        logger.info(f"Retrieved {len(df)} records")
        return df
    
    def get_property_type_comparison(self, region: str = None) -> pd.DataFrame:
        """Query: Apartment vs Independent houses comparison"""
        logger.info("Analyzing property type comparison")
        
        query = """
            SELECT 
                region,
                property_type,
                COUNT(*) as count,
                MIN(price) as min_price,
                MAX(price) as max_price,
                AVG(price) as avg_price,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) as median_price,
                AVG(CASE WHEN sqft > 0 THEN price/sqft END) as price_per_sqft
            FROM listings
            WHERE property_type IN ('apartment', 'independent', 'condo')
        """
        
        if region:
            query += f" AND region = '{region}'"
        
        query += """
            GROUP BY region, property_type
            ORDER BY region, count DESC
        """
        
        df = pd.read_sql(query, self.engine)
        logger.info(f"Retrieved {len(df)} records")
        return df
    
    def get_ownership_distribution(self, region: str = None) -> pd.DataFrame:
        """Query: Locals vs Non-locals ownership percentage"""
        logger.info("Analyzing ownership distribution")
        
        query = """
            SELECT 
                region,
                EXTRACT(YEAR FROM date) as year,
                ownership,
                COUNT(*) as count,
                ROUND(
                    COUNT(*) * 100.0 / 
                    SUM(COUNT(*)) OVER (PARTITION BY region, EXTRACT(YEAR FROM date)), 
                    2
                ) as percentage
            FROM listings
            WHERE ownership != 'unknown'
        """
        
        if region:
            query += f" AND region = '{region}'"
        
        query += """
            GROUP BY region, EXTRACT(YEAR FROM date), ownership
            ORDER BY region, year, ownership
        """
        
        df = pd.read_sql(query, self.engine)
        logger.info(f"Retrieved {len(df)} records")
        return df
    
    def get_yearly_price_trends(self, region: str = None) -> pd.DataFrame:
        """Query: Yearly median house price trend"""
        logger.info("Analyzing yearly price trends")
        
        query = """
            SELECT 
                region,
                EXTRACT(YEAR FROM date) as year,
                COUNT(*) as listing_count,
                AVG(price) as avg_price,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) as median_price,
                MIN(price) as min_price,
                MAX(price) as max_price,
                STDDEV(price) as price_stddev
            FROM listings
            WHERE price IS NOT NULL
        """
        
        if region:
            query += f" AND region = '{region}'"
        
        query += """
            GROUP BY region, EXTRACT(YEAR FROM date)
            ORDER BY region, year
        """
        
        df = pd.read_sql(query, self.engine)
        logger.info(f"Retrieved {len(df)} records")
        return df
    
    def get_monthly_time_series(self, region: str = None) -> pd.DataFrame:
        """Get monthly price time series for forecasting"""
        logger.info("Preparing monthly time series data")
        
        query = """
            SELECT 
                DATE_TRUNC('month', date) as month,
                region,
                COUNT(*) as count,
                AVG(price) as avg_price,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) as median_price
            FROM listings
            WHERE date >= '2000-01-01' AND price IS NOT NULL
        """
        
        if region:
            query += f" AND region = '{region}'"
        
        query += """
            GROUP BY DATE_TRUNC('month', date), region
            ORDER BY month
        """
        
        df = pd.read_sql(query, self.engine)
        df['month'] = pd.to_datetime(df['month'], utc=True)
        df['month'] = pd.to_datetime(df['month'], utc=True).dt.to_period("M")
        logger.info(f"Retrieved {len(df)} monthly records")
        return df
    
    def calculate_yoy_growth(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate year-over-year growth rates"""
        logger.info("Calculating YoY growth rates")
        
        df_sorted = df.sort_values('year')
        df_sorted['yoy_growth_pct'] = df_sorted['median_price'].pct_change() * 100
        
        return df_sorted
    
    def calculate_correlation(self, region: str = 'florida') -> pd.DataFrame:
        """Calculate correlation between price, tax, and macro indicators"""
        logger.info("Calculating correlations")
        
        query = """
            SELECT 
                l.date,
                l.price,
                l.tax,
                m.mortgage_rate,
                m.population
            FROM listings l
            LEFT JOIN macro_indicators m 
                ON l.region = m.region 
                AND DATE_TRUNC('month', l.date) = DATE_TRUNC('month', m.date)
            WHERE l.region = :region
                AND l.price IS NOT NULL
                AND l.tax IS NOT NULL
        """
        
        df = pd.read_sql(query, self.engine, params={'region': region})
        
        if len(df) > 0:
            correlation_matrix = df[['price', 'tax', 'mortgage_rate', 'population']].corr()
            logger.info("Correlation matrix calculated")
            return correlation_matrix
        else:
            logger.warning("Insufficient data for correlation analysis")
            return pd.DataFrame()
    
    def export_analysis_results(self):
        """Export all analysis results to CSV files"""
        logger.info("Exporting analysis results")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Export rent vs own
        df_rent_own = self.get_rent_vs_own_distribution()
        output_path = config.PROCESSED_DATA_DIR / f'rent_vs_own_{timestamp}.csv'
        df_rent_own.to_csv(output_path, index=False)
        logger.info(f"Exported rent vs own to {output_path}")
        
        # Export property types
        df_property = self.get_property_type_comparison()
        output_path = config.PROCESSED_DATA_DIR / f'property_types_{timestamp}.csv'
        df_property.to_csv(output_path, index=False)
        logger.info(f"Exported property types to {output_path}")
        
        # Export ownership
        df_ownership = self.get_ownership_distribution()
        output_path = config.PROCESSED_DATA_DIR / f'ownership_{timestamp}.csv'
        df_ownership.to_csv(output_path, index=False)
        logger.info(f"Exported ownership to {output_path}")
        
        # Export yearly trends
        df_yearly = self.get_yearly_price_trends()
        df_yearly_growth = self.calculate_yoy_growth(df_yearly)
        output_path = config.PROCESSED_DATA_DIR / f'yearly_trends_{timestamp}.csv'
        df_yearly_growth.to_csv(output_path, index=False)
        logger.info(f"Exported yearly trends to {output_path}")
        
        # Export time series
        df_timeseries = self.get_monthly_time_series()
        output_path = config.PROCESSED_DATA_DIR / f'monthly_timeseries_{timestamp}.csv'
        df_timeseries.to_csv(output_path, index=False)
        logger.info(f"Exported time series to {output_path}")
        
        print("\n" + "=" * 80)
        print("ANALYSIS RESULTS EXPORTED")
        print("=" * 80)
        print(f"Location: {config.PROCESSED_DATA_DIR}")
        print(f"Files created: 5")
    
    def generate_summary_report(self) -> str:
        """Generate a text summary report"""
        logger.info("Generating summary report")
        
        report = []
        report.append("=" * 80)
        report.append("REAL ESTATE ANALYTICS SUMMARY REPORT")
        report.append("=" * 80)
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        # Database statistics
        query = "SELECT COUNT(*) as total FROM listings"
        total_listings = pd.read_sql(query, self.engine).iloc[0]['total']
        report.append(f"Total Listings in Database: {total_listings:,}")
        
        query = "SELECT COUNT(DISTINCT region) as regions FROM listings"
        total_regions = pd.read_sql(query, self.engine).iloc[0]['regions']
        report.append(f"Total Regions: {total_regions}")
        
        query = "SELECT MIN(date) as earliest, MAX(date) as latest FROM listings"
        date_range = pd.read_sql(query, self.engine).iloc[0]
        report.append(f"Date Range: {date_range['earliest']} to {date_range['latest']}\n")
        
        # Price statistics
        df_yearly = self.get_yearly_price_trends()
        if len(df_yearly) > 0:
            latest_year = df_yearly['year'].max()
            latest_data = df_yearly[df_yearly['year'] == latest_year]
            
            report.append(f"Latest Year ({int(latest_year)}) Statistics:")
            report.append(f"  Average Price: ${latest_data['avg_price'].mean():,.2f}")
            report.append(f"  Median Price: ${latest_data['median_price'].mean():,.2f}")
            report.append(f"  Listings: {int(latest_data['listing_count'].sum()):,}\n")
        
        report_text = "\n".join(report)
        print(report_text)
        
        # Save report
        output_path = config.OUTPUT_DIR / f'summary_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt'
        with open(output_path, 'w') as f:
            f.write(report_text)
        
        logger.info(f"Summary report saved to {output_path}")
        return report_text
    
    def close(self):
        """Close database connection"""
        self.engine.dispose()
        logger.info("Analytics engine closed")


def main():
    """Main analytics execution"""
    logger.info("=" * 80)
    logger.info("REAL ESTATE ANALYTICS - Starting Analysis")
    logger.info("=" * 80)
    
    analytics = RealEstateAnalytics()
    
    try:
        # Run all analytics
        logger.info("Running comprehensive analytics...")
        
        # Export results
        analytics.export_analysis_results()
        
        # Generate summary report
        analytics.generate_summary_report()
        
        logger.info("Analytics completed successfully")
        
    except Exception as e:
        logger.error(f"Analytics failed: {e}")
        raise
    
    finally:
        analytics.close()


if __name__ == "__main__":
    main()
