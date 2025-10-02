import logging
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from statsmodels.tsa.arima.model import ARIMA
from datetime import datetime
import warnings
import config

warnings.filterwarnings('ignore')

logging.basicConfig(level=config.LOG_CONFIG['level'], format=config.LOG_CONFIG['format'],
    handlers=[logging.FileHandler(config.LOG_CONFIG['file']), logging.StreamHandler()])
logger = logging.getLogger(__name__)

class TimeSeriesForecaster:
    def __init__(self, region='FL'):
        self.region = region
        self.engine = create_engine(config.DB_CONNECTION_STRING)
        self.horizon = 24
        logger.info(f"Forecaster initialized for {region}")
    
    def fetch_time_series(self):
        logger.info("Fetching time series")
        query = f"SELECT DATE_TRUNC('month', date)::date as month, AVG(price) as avg_price FROM listings WHERE region = '{self.region}' AND price IS NOT NULL GROUP BY month ORDER BY month"
        df = pd.read_sql(query, self.engine)
        df['month'] = pd.to_datetime(df['month'])
        df.set_index('month', inplace=True)
        logger.info(f"Retrieved {len(df)} records")
        return df
    
    def run_forecast(self):
        df = self.fetch_time_series()
        if len(df) < 24:
            logger.error("Insufficient data")
            return None
        
        series = df['avg_price']
        split = int(len(series) * 0.8)
        train = series[:split]
        
        logger.info("Fitting ARIMA model")
        model = ARIMA(train, order=(1,1,1))
        fitted = model.fit()
        
        forecast = fitted.forecast(steps=self.horizon)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = config.FORECASTS_DIR / f'forecast_{self.region}_{timestamp}.csv'
        forecast.to_csv(output_path)
        logger.info(f"Saved forecast to {output_path}")
        
        plt.figure(figsize=(12,6))
        plt.plot(train.index, train.values, label='Training')
        plt.plot(forecast.index, forecast.values, label='Forecast', linestyle='--')
        plt.legend()
        plt.title(f'Price Forecast - {self.region}')
        
        chart_path = config.CHARTS_DIR / f'forecast_{self.region}_{timestamp}.png'
        plt.savefig(chart_path, dpi=300, bbox_inches='tight')
        logger.info(f"Saved chart to {chart_path}")
        plt.close()
        
        print(f"\nForecast created successfully!")
        print(f"CSV: {output_path}")
        print(f"Chart: {chart_path}")
        return forecast

def main():
    forecaster = TimeSeriesForecaster('FL')
    forecaster.run_forecast()

if __name__ == "__main__":
    main()
