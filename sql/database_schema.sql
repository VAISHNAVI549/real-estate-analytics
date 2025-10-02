-- Real Estate Analytics Database Schema

DROP TABLE IF EXISTS listings CASCADE;
DROP TABLE IF EXISTS households CASCADE;
DROP TABLE IF EXISTS macro_indicators CASCADE;
DROP TABLE IF EXISTS forecasts CASCADE;

CREATE TABLE listings (
    id SERIAL PRIMARY KEY,
    listing_id VARCHAR(100) UNIQUE NOT NULL,
    date DATE NOT NULL,
    region VARCHAR(100) NOT NULL,
    city VARCHAR(100),
    property_type VARCHAR(50) CHECK (property_type IN ('apartment', 'independent', 'condo', 'townhouse', 'other')),
    price DECIMAL(12, 2) CHECK (price >= 0),
    tax DECIMAL(10, 2),
    sale_type VARCHAR(50) CHECK (sale_type IN ('sale', 'rent', 'lease')),
    ownership VARCHAR(50) CHECK (ownership IN ('local', 'non-local', 'unknown')),
    bedrooms INTEGER CHECK (bedrooms >= 0),
    bathrooms DECIMAL(3, 1) CHECK (bathrooms >= 0),
    sqft INTEGER CHECK (sqft > 0),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE households (
    id SERIAL PRIMARY KEY,
    region VARCHAR(100) NOT NULL,
    city VARCHAR(100),
    year INTEGER NOT NULL,
    family_type VARCHAR(50) CHECK (family_type IN ('single', 'independent', 'joint', 'other')),
    count INTEGER CHECK (count >= 0),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(region, city, year, family_type)
);

CREATE TABLE macro_indicators (
    id SERIAL PRIMARY KEY,
    region VARCHAR(100) NOT NULL,
    date DATE NOT NULL,
    mortgage_rate DECIMAL(5, 2),
    cpi DECIMAL(10, 2),
    population INTEGER,
    unemployment_rate DECIMAL(5, 2),
    median_income DECIMAL(12, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(region, date)
);

CREATE TABLE forecasts (
    id SERIAL PRIMARY KEY,
    region VARCHAR(100) NOT NULL,
    forecast_date DATE NOT NULL,
    metric_type VARCHAR(50) NOT NULL,
    property_type VARCHAR(50),
    predicted_value DECIMAL(12, 2),
    lower_bound DECIMAL(12, 2),
    upper_bound DECIMAL(12, 2),
    model_name VARCHAR(100),
    confidence_level DECIMAL(4, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_listings_date ON listings(date);
CREATE INDEX idx_listings_region ON listings(region);
CREATE INDEX idx_listings_city ON listings(city);
CREATE INDEX idx_listings_property_type ON listings(property_type);
CREATE INDEX idx_listings_sale_type ON listings(sale_type);
CREATE INDEX idx_households_region_year ON households(region, year);
CREATE INDEX idx_macro_region_date ON macro_indicators(region, date);
CREATE INDEX idx_forecasts_region_date ON forecasts(region, forecast_date);

CREATE VIEW rent_vs_own AS
SELECT 
    region,
    EXTRACT(YEAR FROM date) as year,
    sale_type,
    COUNT(*) as count,
    AVG(price) as avg_price,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) as median_price
FROM listings
GROUP BY region, EXTRACT(YEAR FROM date), sale_type;

CREATE VIEW property_type_stats AS
SELECT 
    region,
    property_type,
    COUNT(*) as count,
    AVG(price) as avg_price,
    AVG(sqft) as avg_sqft,
    AVG(price/NULLIF(sqft, 0)) as price_per_sqft
FROM listings
WHERE sqft > 0
GROUP BY region, property_type;

CREATE VIEW ownership_distribution AS
SELECT 
    region,
    EXTRACT(YEAR FROM date) as year,
    ownership,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY region, EXTRACT(YEAR FROM date)), 2) as percentage
FROM listings
GROUP BY region, EXTRACT(YEAR FROM date), ownership;

CREATE VIEW yearly_price_trends AS
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
GROUP BY region, EXTRACT(YEAR FROM date)
ORDER BY region, year;
