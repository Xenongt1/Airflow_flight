-- Metadata table to track ML model training history
-- This helps us avoid retraining when there's no new data

CREATE TABLE IF NOT EXISTS ml_metadata.model_training_log (
    training_id SERIAL PRIMARY KEY,
    model_name VARCHAR(100) NOT NULL,
    training_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    data_max_timestamp TIMESTAMP NOT NULL, -- Latest data point used in training
    records_trained_on INTEGER,
    model_version VARCHAR(50),
    r2_score FLOAT,
    mse FLOAT,
    notes TEXT
);

-- Index for faster lookups
CREATE INDEX IF NOT EXISTS idx_training_timestamp ON ml_metadata.model_training_log (training_timestamp DESC);

-- Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS ml_metadata;