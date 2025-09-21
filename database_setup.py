"""
Database setup script for F1 Analytics project
Creates SQLite database and tables from schema
"""

import sqlite3
import os

def create_database():

    # ensure that data directory exists
    if not os.path.exists('data'):
        os.makedirs('data')

    # connect to SQLite database
    conn = sqlite3.connext('data/f1_database.db')
    cursor = conn.cursor()

    # reading and executing schema
    schema_sql = """
    -- F1 Analytics Database Schema
    
    -- Drivers Table
    CREATE TABLE IF NOT EXISTS drivers (
    driver_id INTEGER PRIMARY KEY,
    driver_ref VARCHAR(30) UNIQUE NOT NULL,
    number INTEGER,
    code VARCHAR(3),
    forename VARCHAR(50) NOT NULL,
    surname VARCHAR(50) NOT NULL,
    dob DATE,
    nationality VARCHAR(50),
    url VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """