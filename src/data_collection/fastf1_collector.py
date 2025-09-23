""" 
Fast-F1 data collector module.
Collects detailed F1 data using the Fast-F1 library.
Includes lap times, telemetry, weather, and session data
"""

import fastf1
import pandas as pd
import sqlite3
import numpy as np
import warnings
from datetime import datetime, timedelta
import os
import time

warnings.filterwarnings("ignore")

class FastF1Collector:
    """ Collector for F1 Data using FastF1 library """
    
    def __init__(self, db_path='data/f1_database.db', cache_dir='data/fastf1_cache'):
        self.db_path = self._find_db_path(db_path)
        self.cache_dir = cache_dir

        # create cache directory
        os.makedirs(cache_dir, exist_ok=True)

        # enable fastf1 cache for faster data retrieval
        fastf1.Cache.enable_cache(cache_dir)

        print(f"Database path set to: {self.db_path}")
        print(f"Cache directory set to: {self.cache_dir}")

    def _find_db_path(self, db_path):
        """ find the correct db path """
        if os.path.exists(db_path):
            return db_path
        
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = current_dir
        while project_root != '/':
            test_path = os.path.join(project_root, 'data', 'f1_database.db')
            if os.path.exists(test_path):
                return test_path
            project_root = os.path.dirname(project_root)

        return db_path
    
    def get_season_schedule(self, year=2024):
        """ get the race schedule for a season """
        print(f"Fetching schedule for {year} season...")

        try: 
            schedule = fastf1.get_event_schedule(year)

            races_data = []
            for idx, event in schedule.iterrows():
                if pd.notna(event['Session5Date']):
                    race_data = {
                        'race_id': f"{year}_{event['RoundNumber']}",
                        'year': year,
                        'round': event['RoundNumber'],
                        'name': event['EventName'],
                        'date': event['Session5Date'].strftime('%Y-%m-%d'),
                        'time': event['Session5Time'].strftime('%H:%M:%S'),
                        'country': event['Country'],
                        'location': event['Location'],
                        'circuit_name': event['CircuitName']
                    }
                    races_data.append(race_data)

            print(f"Found {len(races_data)} races for {year}")
            return races_data
        
        except Exception as e:
            print(f"Error fetching schedule for {year}: {e}")
            return []
        
    def get_session_data(self, year, round_number, session_type='R'):
        """
        Get detailed session data
        session_type: 'FP1', 'FP2', 'FP3', 'Q', 'S' (Sprint), 'R' (Race)
        """
        print(f"Loading {year} Round {round_number} {session_type} session...")
        
        try:
            session = fastf1.get_session(year, round_number, session_type)
            session.load(laps=True, telemetry=False, weather=True)
            
            return session
            
        except Exception as e:
            print(f"Error loading session: {e}")
            return None
        

    def extract_lap_data(self, session):
        """Extract detailed lap data from session"""
        if session is None:
            return pd.DataFrame()
        
        try:
            laps = session.laps
            
            if laps.empty:
                return pd.DataFrame()
            
            # Add race information
            laps['race_year'] = session.event.year
            laps['race_round'] = session.event.RoundNumber
            laps['race_id'] = f"{session.event.year}_{session.event.RoundNumber}"
            laps['session_type'] = session.name
            
            # Convert time columns to total seconds for easier analysis
            time_columns = ['LapTime', 'Sector1Time', 'Sector2Time', 'Sector3Time']
            for col in time_columns:
                if col in laps.columns:
                    laps[f'{col}_seconds'] = laps[col].dt.total_seconds()
            
            # Clean up driver names and add identifiers
            laps['driver_code'] = laps['Driver']
            laps['driver_number'] = laps['DriverNumber']
            
            return laps
            
        except Exception as e:
            print(f"Error extracting lap data: {e}")
            return pd.DataFrame()
        

    def extract_race_results(self, session):
        """Extract race results from session"""
        if session is None:
            return pd.DataFrame()
        
        try:
            results = session.results
            
            if results.empty:
                return pd.DataFrame()
            
            # Add race information
            results['race_year'] = session.event.year
            results['race_round'] = session.event.RoundNumber
            results['race_id'] = f"{session.event.year}_{session.event.RoundNumber}"
            
            # Convert time to seconds
            if 'Time' in results.columns:
                results['race_time_seconds'] = results['Time'].dt.total_seconds()
            
            return results
            
        except Exception as e:
            print(f"Error extracting results: {e}")
            return pd.DataFrame()
    
    def extract_weather_data(self, session):
        """Extract weather data from session"""
        if session is None:
            return pd.DataFrame()
        
        try:
            weather = session.weather_data
            
            if weather.empty:
                return pd.DataFrame()
            
            # Add race information
            weather['race_year'] = session.event.year
            weather['race_round'] = session.event.RoundNumber
            weather['race_id'] = f"{session.event.year}_{session.event.RoundNumber}"
            weather['session_type'] = session.name
            
            return weather
            
        except Exception as e:
            print(f"Error extracting weather: {e}")
            return pd.DataFrame()
        
    def collect_race_weekend_data(self, year, round_number):
        """Collect comprehensive data for a race weekend"""
        print(f"\nCollecting data for {year} Round {round_number}")
        
        weekend_data = {
            'race_results': pd.DataFrame(),
            'qualifying_results': pd.DataFrame(),
            'race_laps': pd.DataFrame(),
            'qualifying_laps': pd.DataFrame(),
            'weather': pd.DataFrame()
        }
        
        # Get race session
        race_session = self.get_session_data(year, round_number, 'R')
        if race_session:
            weekend_data['race_results'] = self.extract_race_results(race_session)
            weekend_data['race_laps'] = self.extract_lap_data(race_session)
            race_weather = self.extract_weather_data(race_session)
            if not race_weather.empty:
                weekend_data['weather'] = race_weather
        
        # Get qualifying session
        quali_session = self.get_session_data(year, round_number, 'Q')
        if quali_session:
            weekend_data['qualifying_results'] = self.extract_race_results(quali_session)
            weekend_data['qualifying_laps'] = self.extract_lap_data(quali_session)
            quali_weather = self.extract_weather_data(quali_session)
            if not quali_weather.empty and weekend_data['weather'].empty:
                weekend_data['weather'] = quali_weather
        
        return weekend_data
    
    def save_enhanced_data(self, weekend_data):
        """Save enhanced data to new tables"""
        conn = sqlite3.connect(self.db_path)
        
        try:
            # Save lap times data
            if not weekend_data['race_laps'].empty:
                laps_df = weekend_data['race_laps'].copy()
                
                # Select relevant columns for database
                lap_columns = [
                    'race_id', 'driver_code', 'driver_number', 'LapNumber', 
                    'LapTime_seconds', 'Sector1Time_seconds', 'Sector2Time_seconds', 
                    'Sector3Time_seconds', 'SpeedI1', 'SpeedI2', 'SpeedFL', 'SpeedST',
                    'IsPersonalBest', 'Compound', 'TyreLife', 'TrackStatus'
                ]
                
                # Only keep columns that exist
                available_columns = [col for col in lap_columns if col in laps_df.columns]
                laps_subset = laps_df[available_columns]
                
                # Create enhanced lap times table if it doesn't exist
                conn.execute("""
                CREATE TABLE IF NOT EXISTS enhanced_lap_times (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    race_id TEXT,
                    driver_code TEXT,
                    driver_number INTEGER,
                    lap_number INTEGER,
                    lap_time_seconds REAL,
                    sector1_time_seconds REAL,
                    sector2_time_seconds REAL,
                    sector3_time_seconds REAL,
                    speed_i1 REAL,
                    speed_i2 REAL,
                    speed_fl REAL,
                    speed_st REAL,
                    is_personal_best BOOLEAN,
                    compound TEXT,
                    tyre_life INTEGER,
                    track_status TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """)
                
                # Save data
                laps_subset.to_sql('enhanced_lap_times', conn, if_exists='append', index=False)
                print(f"Saved {len(laps_subset)} lap records")
            
            # Save weather data
            if not weekend_data['weather'].empty:
                weather_df = weekend_data['weather'].copy()
                
                # Create weather table if it doesn't exist
                conn.execute("""
                CREATE TABLE IF NOT EXISTS enhanced_weather (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    race_id TEXT,
                    session_type TEXT,
                    time_stamp TEXT,
                    air_temp REAL,
                    humidity REAL,
                    pressure REAL,
                    rainfall REAL,
                    track_temp REAL,
                    wind_direction REAL,
                    wind_speed REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """)
                
                weather_df.to_sql('enhanced_weather', conn, if_exists='append', index=False)
                print(f"Saved {len(weather_df)} weather records")
            
            # Save enhanced race results
            if not weekend_data['race_results'].empty:
                results_df = weekend_data['race_results'].copy()
                
                # Create enhanced results table
                conn.execute("""
                CREATE TABLE IF NOT EXISTS enhanced_race_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    race_id TEXT,
                    driver_code TEXT,
                    driver_number INTEGER,
                    position INTEGER,
                    points REAL,
                    grid_position INTEGER,
                    race_time_seconds REAL,
                    status TEXT,
                    team_name TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """)
                
                # Select relevant columns
                result_columns = [
                    'race_id', 'Abbreviation', 'DriverNumber', 'Position', 
                    'Points', 'GridPosition', 'race_time_seconds', 'Status', 'TeamName'
                ]
                available_result_columns = [col for col in result_columns if col in results_df.columns]
                results_subset = results_df[available_result_columns]
                
                results_subset.to_sql('enhanced_race_results', conn, if_exists='append', index=False)
                print(f"Saved {len(results_subset)} race results")
            
            conn.commit()
            
        except Exception as e:
            print(f"Error saving data: {e}")
        finally:
            conn.close()
    
    def collect_recent_season_data(self, year=2024, max_rounds=5):
        """Collect data for recent races"""
        print(f"Starting comprehensive data collection for {year}")
        
        # Get schedule first
        schedule = self.get_season_schedule(year)
        
        if not schedule:
            print("No schedule data available")
            return
        
        # Process recent races (limit for demo)
        completed_races = []
        for race in schedule[:max_rounds]:
            try:
                race_date = datetime.strptime(race['date'], '%Y-%m-%d')
                if race_date <= datetime.now():
                    completed_races.append(race)
            except:
                continue
        
        print(f"Found {len(completed_races)} completed races to process")
        
        for i, race in enumerate(completed_races, 1):
            print(f"\n--- Processing Race {i}/{len(completed_races)}: {race['name']} ---")
            
            weekend_data = self.collect_race_weekend_data(year, race['round'])
            
            if any(not df.empty for df in weekend_data.values()):
                self.save_enhanced_data(weekend_data)
                print(f"Completed {race['name']}")
            else:
                print(f"No data available for {race['name']}")
            
            # Small delay to be respectful
            time.sleep(1)
        
        print(f"\nData collection complete! Collected data for {len(completed_races)} races")

def main():
    """Main collection script"""
    collector = FastF1Collector()
    
    # Collect 2024 season data (limit to 3 races for demo)
    collector.collect_recent_season_data(year=2024, max_rounds=3)
    
    print("\nFastF1 data collection complete!")
    print("🔍 Ready to build predictive models with real F1 data!")

if __name__ == "__main__":
    main()


