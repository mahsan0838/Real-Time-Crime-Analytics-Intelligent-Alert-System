CREATE TABLE IF NOT EXISTS crime_trends (
    id SERIAL PRIMARY KEY,
    year INT,
    month INT,
    day_of_week VARCHAR(10),
    hour INT,
    crime_count BIGINT
);

CREATE TABLE IF NOT EXISTS hotspots (
    id SERIAL PRIMARY KEY,
    cluster_id INT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    crime_count INT
);

CREATE TABLE IF NOT EXISTS correlations (
    id SERIAL PRIMARY KEY,
    correlation_type VARCHAR(100),
    district INT,
    value DOUBLE PRECISION,
    metadata JSONB
);

CREATE TABLE IF NOT EXISTS alerts (
    id SERIAL PRIMARY KEY,
    district INT,
    timestamp TIMESTAMP,
    event_count INT,
    threshold INT,
    severity VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS arrest_rates (
    id SERIAL PRIMARY KEY,
    crime_type VARCHAR(100),
    district INT,
    race VARCHAR(50),
    arrest_rate DECIMAL(5,2)
);

-- Violence analytics (7.3)
CREATE TABLE IF NOT EXISTS violence_stats (
    id SERIAL PRIMARY KEY,
    month INT,
    district INT,
    homicides_count BIGINT,
    nonfatal_shootings_count BIGINT,
    gunshot_injury_rate DECIMAL(6,4),
    top_community_area INT,
    top_community_area_count BIGINT
);

-- Sex offender density (7.4)
CREATE TABLE IF NOT EXISTS sex_offender_density (
    id SERIAL PRIMARY KEY,
    district INT,
    offender_count BIGINT,
    victim_minor_count BIGINT
);