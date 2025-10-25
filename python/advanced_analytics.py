"""
Advanced Analytics Module - Additional visualizations and insights
"""
import logging
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import config

logging.basicConfig(
    level=config.LOG_CONFIG['level'],
    format=config.LOG_CONFIG['format'],
    handlers=[
        logging.FileHandler(config.LOG_CONFIG['file']),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")


class AdvancedRealEstateAnalytics:
    """Generate advanced visualizations and analytics"""
    
    def __init__(self):
        self.engine = create_engine(config.DB_CONNECTION_STRING)
        logger.info("Advanced Analytics initialized")
    
    def create_regional_comparison_bar(self):
        """Bar chart comparing median prices across top regions"""
        logger.info("Creating regional comparison bar chart")
        
        query = """
            SELECT 
                region,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) as median_price,
                COUNT(*) as listing_count
            FROM listings
            WHERE price IS NOT NULL
            GROUP BY region
            HAVING COUNT(*) > 100
            ORDER BY median_price DESC
            LIMIT 15
        """
        
        df = pd.read_sql(query, self.engine)
        
        fig, ax = plt.subplots(figsize=(14, 8))
        
        bars = ax.barh(df['region'], df['median_price'], color=sns.color_palette("rocket", len(df)))
        
        ax.set_xlabel('Median Price ($)', fontsize=12, fontweight='bold')
        ax.set_ylabel('State', fontsize=12, fontweight='bold')
        ax.set_title('Top 15 States by Median House Price', fontsize=16, fontweight='bold', pad=20)
        
        for i, (price, count) in enumerate(zip(df['median_price'], df['listing_count'])):
            ax.text(price + 10000, i, f'${price:,.0f}\n({count:,} listings)', 
                   va='center', fontsize=9, fontweight='bold')
        
        ax.grid(axis='x', alpha=0.3, linestyle='--')
        ax.set_xlim(0, df['median_price'].max() * 1.15)
        
        plt.tight_layout()
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = config.CHARTS_DIR / f'regional_comparison_bar_{timestamp}.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        logger.info(f"Saved: {output_path}")
        plt.close()
        
        return df
    
    def create_year_over_year_trend(self):
        """Line chart showing price trends over years with multiple metrics"""
        logger.info("Creating year-over-year trend chart")
        
        query = """
            SELECT 
                EXTRACT(YEAR FROM date) as year,
                AVG(price) as avg_price,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) as median_price,
                COUNT(*) as listing_count
            FROM listings
            WHERE price IS NOT NULL AND date >= '2000-01-01'
            GROUP BY EXTRACT(YEAR FROM date)
            ORDER BY year
        """
        
        df = pd.read_sql(query, self.engine)
        
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))
        
        # Price trends
        ax1.plot(df['year'], df['avg_price'], marker='o', linewidth=2.5, 
                label='Average Price', color='#e74c3c', markersize=8)
        ax1.plot(df['year'], df['median_price'], marker='s', linewidth=2.5, 
                label='Median Price', color='#3498db', markersize=8)
        
        ax1.set_xlabel('Year', fontsize=12, fontweight='bold')
        ax1.set_ylabel('Price ($)', fontsize=12, fontweight='bold')
        ax1.set_title('Historical Price Trends (2000-Present)', fontsize=16, fontweight='bold', pad=15)
        ax1.legend(fontsize=11, loc='upper left')
        ax1.grid(True, alpha=0.3, linestyle='--')
        ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x/1000:.0f}K'))
        
        # Listing volume
        ax2.bar(df['year'], df['listing_count'], color='#2ecc71', alpha=0.7, edgecolor='black')
        ax2.set_xlabel('Year', fontsize=12, fontweight='bold')
        ax2.set_ylabel('Number of Listings', fontsize=12, fontweight='bold')
        ax2.set_title('Listing Volume Over Time', fontsize=16, fontweight='bold', pad=15)
        ax2.grid(axis='y', alpha=0.3, linestyle='--')
        ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x/1000:.0f}K'))
        
        plt.tight_layout()
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = config.CHARTS_DIR / f'year_over_year_trends_{timestamp}.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        logger.info(f"Saved: {output_path}")
        plt.close()
        
        return df
    
    def create_property_type_pie(self):
        """Pie chart showing distribution of property types"""
        logger.info("Creating property type pie chart")
        
        query = """
            SELECT 
                property_type,
                COUNT(*) as count
            FROM listings
            WHERE property_type IS NOT NULL
            GROUP BY property_type
            ORDER BY count DESC
        """
        
        df = pd.read_sql(query, self.engine)
        
        colors = sns.color_palette('Set3', len(df))
        
        fig, ax = plt.subplots(figsize=(12, 8))
        
        wedges, texts, autotexts = ax.pie(
            df['count'], 
            labels=df['property_type'],
            autopct=lambda pct: f'{pct:.1f}%\n({int(pct/100 * df["count"].sum()):,})',
            colors=colors,
            startangle=90,
            textprops={'fontsize': 11, 'fontweight': 'bold'},
            explode=[0.05] * len(df)
        )
        
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontsize(10)
        
        ax.set_title('Property Type Distribution', fontsize=18, fontweight='bold', pad=20)
        
        plt.tight_layout()
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = config.CHARTS_DIR / f'property_type_pie_{timestamp}.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        logger.info(f"Saved: {output_path}")
        plt.close()
        
        return df
    
    def create_monthly_seasonality_analysis(self):
        """Bar chart showing seasonal patterns in pricing"""
        logger.info("Creating monthly seasonality analysis")
        
        query = """
            SELECT 
                EXTRACT(MONTH FROM date) as month,
                AVG(price) as avg_price,
                COUNT(*) as listing_count
            FROM listings
            WHERE price IS NOT NULL
            GROUP BY EXTRACT(MONTH FROM date)
            ORDER BY month
        """
        
        df = pd.read_sql(query, self.engine)
        
        month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                      'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        df['month_name'] = df['month'].apply(lambda x: month_names[int(x)-1])
        
        fig, ax = plt.subplots(figsize=(14, 7))
        
        bars = ax.bar(df['month_name'], df['avg_price'], 
                     color=sns.color_palette("coolwarm", len(df)), 
                     edgecolor='black', linewidth=1.5)
        
        for bar in bars:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'${height:,.0f}',
                   ha='center', va='bottom', fontsize=10, fontweight='bold')
        
        ax.set_xlabel('Month', fontsize=12, fontweight='bold')
        ax.set_ylabel('Average Price ($)', fontsize=12, fontweight='bold')
        ax.set_title('Seasonal Price Patterns by Month', fontsize=16, fontweight='bold', pad=20)
        ax.grid(axis='y', alpha=0.3, linestyle='--')
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x/1000:.0f}K'))
        
        plt.tight_layout()
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = config.CHARTS_DIR / f'monthly_seasonality_{timestamp}.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        logger.info(f"Saved: {output_path}")
        plt.close()
        
        return df
    
    def create_price_growth_rate_bar(self):
        """Bar chart showing YoY growth rates"""
        logger.info("Creating price growth rate chart")
        
        query = """
            WITH yearly_prices AS (
                SELECT 
                    EXTRACT(YEAR FROM date) as year,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) as median_price
                FROM listings
                WHERE price IS NOT NULL AND date >= '2000-01-01'
                GROUP BY EXTRACT(YEAR FROM date)
            )
            SELECT 
                year,
                median_price,
                LAG(median_price) OVER (ORDER BY year) as prev_year_price
            FROM yearly_prices
            ORDER BY year
        """
        
        df = pd.read_sql(query, self.engine)
        df = df.dropna()
        df['growth_rate'] = ((df['median_price'] - df['prev_year_price']) / df['prev_year_price']) * 100
        
        fig, ax = plt.subplots(figsize=(14, 7))
        
        colors = ['#2ecc71' if x > 0 else '#e74c3c' for x in df['growth_rate']]
        bars = ax.bar(df['year'], df['growth_rate'], color=colors, edgecolor='black', linewidth=1.5)
        
        for bar, rate in zip(bars, df['growth_rate']):
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'{rate:+.1f}%',
                   ha='center', va='bottom' if height > 0 else 'top',
                   fontsize=9, fontweight='bold')
        
        ax.axhline(y=0, color='black', linestyle='-', linewidth=2)
        ax.set_xlabel('Year', fontsize=12, fontweight='bold')
        ax.set_ylabel('Year-over-Year Growth Rate (%)', fontsize=12, fontweight='bold')
        ax.set_title('Annual Price Growth Rates', fontsize=16, fontweight='bold', pad=20)
        ax.grid(axis='y', alpha=0.3, linestyle='--')
        
        plt.tight_layout()
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = config.CHARTS_DIR / f'price_growth_rates_{timestamp}.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        logger.info(f"Saved: {output_path}")
        plt.close()
        
        return df
    
    def create_top_cities_analysis(self, state='FL'):
        """Bar chart showing top cities within a state"""
        logger.info(f"Creating top cities analysis for {state}")
        
        query = f"""
            SELECT 
                city,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) as median_price,
                COUNT(*) as listing_count
            FROM listings
            WHERE region = '{state}' AND price IS NOT NULL AND city IS NOT NULL
            GROUP BY city
            HAVING COUNT(*) > 10
            ORDER BY median_price DESC
            LIMIT 10
        """
        
        df = pd.read_sql(query, self.engine)
        
        if len(df) == 0:
            logger.warning(f"No data found for state {state}")
            return None
        
        fig, ax = plt.subplots(figsize=(12, 8))
        
        bars = ax.barh(df['city'], df['median_price'], 
                      color=sns.color_palette("viridis", len(df)))
        
        for i, (price, count) in enumerate(zip(df['median_price'], df['listing_count'])):
            ax.text(price + 5000, i, f'${price:,.0f}', 
                   va='center', fontsize=10, fontweight='bold')
        
        ax.set_xlabel('Median Price ($)', fontsize=12, fontweight='bold')
        ax.set_ylabel('City', fontsize=12, fontweight='bold')
        ax.set_title(f'Top 10 Cities in {state} by Median Price', 
                    fontsize=16, fontweight='bold', pad=20)
        ax.grid(axis='x', alpha=0.3, linestyle='--')
        
        plt.tight_layout()
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = config.CHARTS_DIR / f'top_cities_{state}_{timestamp}.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        logger.info(f"Saved: {output_path}")
        plt.close()
        
        return df
    
    def run_all_analytics(self):
        """Run all advanced analytics"""
        logger.info("=" * 80)
        logger.info("ADVANCED ANALYTICS - Starting")
        logger.info("=" * 80)
        
        results = {}
        
        try:
            results['regional_comparison'] = self.create_regional_comparison_bar()
            results['yoy_trends'] = self.create_year_over_year_trend()
            results['property_pie'] = self.create_property_type_pie()
            results['seasonality'] = self.create_monthly_seasonality_analysis()
            results['growth_rates'] = self.create_price_growth_rate_bar()
            results['top_cities'] = self.create_top_cities_analysis('FL')
            
            print("\n" + "=" * 80)
            print("ADVANCED ANALYTICS COMPLETE")
            print("=" * 80)
            print(f"Charts created: {len(results)}")
            print(f"Location: {config.CHARTS_DIR}")
            print("\nGenerated charts:")
            print("  1. Regional Comparison Bar Chart")
            print("  2. Year-over-Year Trends")
            print("  3. Property Type Pie Chart")
            print("  4. Monthly Seasonality Analysis")
            print("  5. Price Growth Rates")
            print("  6. Top Cities Analysis")
            
            logger.info("Advanced analytics completed successfully")
            
        except Exception as e:
            logger.error(f"Advanced analytics failed: {e}")
            raise
        
        return results
    
    def close(self):
        """Close database connection"""
        self.engine.dispose()
        logger.info("Advanced analytics closed")


def main():
    """Main execution"""
    analytics = AdvancedRealEstateAnalytics()
    
    try:
        analytics.run_all_analytics()
    except Exception as e:
        logger.error(f"Execution failed: {e}")
        raise
    finally:
        analytics.close()


if __name__ == "__main__":
    main()
