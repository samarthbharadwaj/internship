#!/usr/bin/env python3


import os
import psycopg2

# Database connection details — pulled from environment variables
DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "airflow")
DB_USER = os.getenv("POSTGRES_USER", "airflow")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "airflow")

TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS stock_data (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    open NUMERIC(10, 4),
    high NUMERIC(10, 4),
    low NUMERIC(10, 4),
    close NUMERIC(10, 4),
    volume BIGINT,
    CONSTRAINT unique_symbol_time UNIQUE(symbol, timestamp)
);
"""

def init_db():
    try:
        # Connect to PostgreSQL
        print(f"Connecting to PostgreSQL at {DB_HOST}:{DB_PORT}...")
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        conn.autocommit = True
        cur = conn.cursor()

        # Create the stock_data table
        print("Creating stock_data table if not exists...")
        cur.execute(TABLE_SCHEMA)
        print(" Database initialized successfully.")

        cur.close()
        conn.close()

    except Exception as e:
        print(" Failed to initialize database:", e)

if __name__ == "__main__":
    init_db()

