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

    -- Constructors Table
    CREATE TABLE IF NOT EXISTS constructors (
        constructor_id INTEGER PRIMARY KEY,
        constructor_ref VARCHAR(30) UNIQUE NOT NULL,
        name VARCHAR(100) NOT NULL,
        nationality VARCHAR(50),
        url VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Circuits Table
    CREATE TABLE IF NOT EXISTS circuits (
        circuit_id INTEGER PRIMARY KEY,
        circuit_ref VARCHAR(30) UNIQUE NOT NULL,
        name VARCHAR(100) NOT NULL,
        location VARCHAR(100),
        country VARCHAR(50),
        lat DECIMAL(10, 6),
        lng DECIMAL(10, 6),
        alt INTEGER,
        url VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Seasons Table
    CREATE TABLE IF NOT EXISTS seasons (
        year INTEGER PRIMARY KEY,
        url VARCHAR(255)
    );

    -- Race Table
    CREATE TABLE IF NOT EXISTS races (
        race_id INTEGER PRIMARY KEY,
        year INTEGER NOT NULL,
        round INTEGER NOT NULL,
        circuit_id INTEGER NOT NULL,
        name VARCHAR(100) NOT NULL,
        date DATE NOT NULL,
        time TIME,
        url VARCHAR(255),
        fp1_date DATE,
        fp1_time TIME,
        fp2_date DATE,
        fp2_time TIME,
        fp3_date DATE,
        fp3_time TIME,
        quali_date DATE,
        quali_time TIME,
        sprint_date DATE,
        sprint_time TIME,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (year) REFERENCES seasons(year),
        FOREIGN KEY (circuit_id) REFERENCES circuits(circuit_id),
        UNIQUE(year, round)
    );

    -- Race Results Table
    CREATE TABLE IF NOT EXISTS race_results (
        result_id INTEGER PRIMARY KEY,
        race_id INTEGER NOT NULL,
        driver_id INTEGER NOT NULL,
        constructor_id INTEGER NOT NULL,
        number INTEGER,
        grid INTEGER,
        position INTEGER,
        position_text VARCHAR(10),
        position_order INTEGER,
        points DECIMAL(8,2) DEFAULT 0,
        laps INTEGER,
        time_milliseconds INTEGER,
        fastest_lap INTEGER,
        fastest_lap_rank INTEGER,
        fastest_lap_time VARCHAR(20),
        fastest_lap_speed DECIMAL(6,3),
        status_id INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (race_id) REFERENCES races(race_id),
        FOREIGN KEY (driver_id) REFERENCES drivers(driver_id),
        FOREIGN KEY (constructor_id) REFERENCES constructors(constructor_id),
        UNIQUE(race_id, driver_id)
    );

    -- Qualifying Results Table
    CREATE TABLE IF NOT EXISTS qualifying_results (
        qualify_id INTEGER PRIMARY KEY,
        race_id INTEGER NOT NULL,
        driver_id INTEGER NOT NULL,
        constructor_id INTEGER NOT NULL,
        number INTEGER,
        position INTEGER,
        q1_time VARCHAR(20),
        q1_milliseconds INTEGER,
        q2_time VARCHAR(20),
        q2_milliseconds INTEGER,
        q3_time VARCHAR(20),
        q3_milliseconds INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (race_id) REFERENCES races(race_id),
        FOREIGN KEY (driver_id) REFERENCES drivers(driver_id),
        FOREIGN KEY (constructor_id) REFERENCES constructors(constructor_id),
        UNIQUE(race_id, driver_id)
    );

    -- Driver standings table
    CREATE TABLE IF NOT EXISTS driver_standings (
        standing_id INTEGER PRIMARY KEY,
        race_id INTEGER NOT NULL,
        driver_id INTEGER NOT NULL,
        points DECIMAL(8,2) DEFAULT 0,
        position INTEGER,
        position_text VARCHAR(10),
        wins INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (race_id) REFERENCES races(race_id),
        FOREIGN KEY (driver_id) REFERENCES drivers(driver_id),
        UNIQUE(race_id, driver_id)
    );

    -- Constructor standings table
    CREATE TABLE IF NOT EXISTS constructor_standings (
        standing_id INTEGER PRIMARY KEY,
        race_id INTEGER NOT NULL,
        constructor_id INTEGER NOT NULL,
        points DECIMAL(8,2) DEFAULT 0,
        position INTEGER,
        position_text VARCHAR(10),
        wins INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (race_id) REFERENCES races(race_id),
        FOREIGN KEY (constructor_id) REFERENCES constructors(constructor_id),
        UNIQUE(race_id, constructor_id)
    );
    
    -- Status table
    CREATE TABLE IF NOT EXISTS status (
        status_id INTEGER PRIMARY KEY,
        status VARCHAR(50) UNIQUE NOT NULL
    );
    
    -- Lap times table
    CREATE TABLE IF NOT EXISTS lap_times (
        lap_time_id INTEGER PRIMARY KEY,
        race_id INTEGER NOT NULL,
        driver_id INTEGER NOT NULL,
        lap INTEGER NOT NULL,
        position INTEGER,
        time_string VARCHAR(20),
        milliseconds INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (race_id) REFERENCES races(race_id),
        FOREIGN KEY (driver_id) REFERENCES drivers(driver_id),
        UNIQUE(race_id, driver_id, lap)
    );
    
    -- Pit stops table
    CREATE TABLE IF NOT EXISTS pit_stops (
        pit_stop_id INTEGER PRIMARY KEY,
        race_id INTEGER NOT NULL,
        driver_id INTEGER NOT NULL,
        stop INTEGER NOT NULL,
        lap INTEGER NOT NULL,
        time_string VARCHAR(20),
        duration_string VARCHAR(20),
        duration_milliseconds INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (race_id) REFERENCES races(race_id),
        FOREIGN KEY (driver_id) REFERENCES drivers(driver_id),
        UNIQUE(race_id, driver_id, stop)
    );

    """

    # Execute schema
    cursor.executescript(schema_sql)

    # Insert initial status values
    status_values = [
        (1, 'Finished'), (2, 'Disqualified'), (3, 'Accident'), (4, 'Collision'), 
        (5, 'Engine'), (6, 'Gearbox'), (7, 'Transmission'), (8, 'Clutch'),
        (9, 'Hydraulics'), (10, 'Electrical'), (11, '+1 Lap'), (12, '+2 Laps'),
        (13, '+3 Laps'), (14, '+4 Laps'), (15, '+5 Laps'), (16, 'Spun off'),
        (17, 'Radiator'), (18, 'Suspension'), (19, 'Brakes'), (20, 'Differential')
    ]

    cursor.executemany(
        "INSERT OR IGNORE INTO status (status_id, status) VALUES (?, ?);", 
        status_values
    )

    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_race_results_race_id ON race_results(race_id)",
        "CREATE INDEX IF NOT EXISTS idx_race_results_driver_id ON race_results(driver_id)",
        "CREATE INDEX IF NOT EXISTS idx_qualifying_results_race_id ON qualifying_results(race_id)",
        "CREATE INDEX IF NOT EXISTS idx_driver_standings_race_id ON driver_standings(race_id)",
        "CREATE INDEX IF NOT EXISTS idx_races_year ON races(year)",
        "CREATE INDEX IF NOT EXISTS idx_races_date ON races(date)"
    ]

    for index in indexes:
        cursor.execute(index)

    conn.commit()
    conn.close()

    print("Database and tables created successfully.")
    print("Location: data/f1_database.db")

    # Display table content
    conn = sqlite3.connect('data/f1_database.db')
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    print(f"📊 Created {len(tables)} tables: {', '.join([t[0] for t in tables])}")
    conn.close()


if __name__ == "__main__":
    create_database()
