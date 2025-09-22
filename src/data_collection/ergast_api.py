""" 
Ergast F1 API Client
Handles data collection from Ergast Developer API
API Documentation: https://ergast.com/mrd/
"""

import requests
import time
import json
import sqlite3
from datetime import datetime
import os

class ErgastClient:
    """ Client for fetching data """
    
    def __init__(self, db_path='data/f1_database.db'):
        self.base_url = 'https://ergast.com/api/f1'
        self.db_path = db_path
        self.session = requests.Session()
        self.request_delay = 0.5 # allows for 0.5 seconds between requests

    def _make_request(self, endpoint):
        """ make a request to the ergast API with error handling """
        url = f"{self.base_url}/{endpoint}.json"

        try:
            time.sleep(self.request_delay)
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"Error fetching data from {url}: {e}")
            return None
        
    def get_seasons(self, start_year=2000, end_year=datetime.now().year):
        """ Fetch seasons from start_year to end_year """
        seasons = []
        data = self._make_request(f"seasons")
        if not data:
            return []
        
        for season in data['MRData']['SeasonTable']['Seasons']:
            year = int(season['season'])
            if start_year <= year <= end_year:
                seasons.append({
                    'year': year,
                    'url': season.get('url', None)
                })
        
        print(f"Fetched {len(seasons)} seasons from {start_year} to {end_year}")
        return seasons
    
    def get_circuits(self):
        """Fetch all circuits"""
        print("Fetching circuits...")
        
        data = self._make_request("circuits")
        if not data:
            return []
        
        circuits = []
        for circuit in data['MRData']['CircuitTable']['Circuits']:
            circuits.append({
                'circuit_id': circuit['circuitId'],
                'circuit_ref': circuit['circuitId'],
                'name': circuit['circuitName'],
                'location': circuit['Location']['locality'],
                'country': circuit['Location']['country'],
                'lat': float(circuit['Location']['lat']) if circuit['Location'].get('lat') else None,
                'lng': float(circuit['Location']['long']) if circuit['Location'].get('long') else None,
                'alt': int(circuit['Location']['alt']) if circuit['Location'].get('alt') else None,
                'url': circuit.get('url', '')
            })
        
        print(f"Found {len(circuits)} circuits")
        return circuits
    
    def get_drivers(self):
        """Fetch all drivers"""
        print("Fetching drivers...")
        
        data = self._make_request("drivers")
        if not data:
            return []
        
        drivers = []
        for driver in data['MRData']['DriverTable']['Drivers']:
            drivers.append({
                'driver_id': driver['driverId'],
                'driver_ref': driver['driverId'],
                'number': int(driver['permanentNumber']) if driver.get('permanentNumber') else None,
                'code': driver.get('code', None),
                'forename': driver['givenName'],
                'surname': driver['familyName'],
                'dob': driver['dateOfBirth'],
                'nationality': driver.get('nationality', None),
                'url': driver.get('url', '')
            })

        print(f"Found {len(drivers)} drivers")
        return drivers
    
    def get_constructors(self):
        """Fetch all constructors"""
        print("Fetching constructors...")
        
        data = self._make_request("constructors")
        if not data:
            return []
        
        constructors = []
        for constructor in data['MRData']['ConstructorTable']['Constructors']:
            constructors.append({
                'constructor_id': constructor['constructorId'],
                'constructor_ref': constructor['constructorId'],
                'name': constructor['name'],
                'nationality': constructor.get('nationality'),
                'url': constructor.get('url', '')
            })
        
        print(f"Found {len(constructors)} constructors")
        return constructors
    
    def get_races_for_season(self, year):
        """Fetch all races for a specific season"""
        print(f"Fetching races for {year}...")
        
        data = self._make_request(f"{year}")
        if not data:
            return []
        
        races = []
        for race in data['MRData']['RaceTable']['Races']:
            races.append({
                'race_id': f"{year}_{race['round']}",
                'year': year,
                'round': int(race['round']),
                'circuit_id': race['Circuit']['circuitId'],
                'name': race['raceName'],
                'date': race['date'],
                'time': race.get('time', '').replace('Z', '') if race.get('time') else None,
                'url': race.get('url', ''),
                # Practice and qualifying sessions
                'fp1_date': race.get('FirstPractice', {}).get('date'),
                'fp1_time': race.get('FirstPractice', {}).get('time', '').replace('Z', '') if race.get('FirstPractice', {}).get('time') else None,
                'fp2_date': race.get('SecondPractice', {}).get('date'),
                'fp2_time': race.get('SecondPractice', {}).get('time', '').replace('Z', '') if race.get('SecondPractice', {}).get('time') else None,
                'fp3_date': race.get('ThirdPractice', {}).get('date'),
                'fp3_time': race.get('ThirdPractice', {}).get('time', '').replace('Z', '') if race.get('ThirdPractice', {}).get('time') else None,
                'quali_date': race.get('Qualifying', {}).get('date'),
                'quali_time': race.get('Qualifying', {}).get('time', '').replace('Z', '') if race.get('Qualifying', {}).get('time') else None,
                'sprint_date': race.get('Sprint', {}).get('date'),
                'sprint_time': race.get('Sprint', {}).get('time', '').replace('Z', '') if race.get('Sprint', {}).get('time') else None,
            })
        
        print(f"Found {len(races)} races for {year}")
        return races
    
    def get_race_results(self, year, round_num):
        """Fetch race results for a specific race"""
        data = self._make_request(f"{year}/{round_num}/results")
        if not data:
            return []
        
        results = []
        race_data = data['MRData']['RaceTable']['Races']
        if not race_data:
            return []
        
        race_id = f"{year}_{round_num}"
        
        for result in race_data[0]['Results']:
            # Handle time - could be race time or gap
            time_ms = None
            if result.get('Time'):
                # This is race time for winner, convert to milliseconds
                time_str = result['Time']['time']
                time_ms = self._time_to_milliseconds(time_str)
            
            results.append({
                'race_id': race_id,
                'driver_id': result['Driver']['driverId'],
                'constructor_id': result['Constructor']['constructorId'],
                'number': int(result.get('number', 0)) if result.get('number') else None,
                'grid': int(result['grid']) if result['grid'].isdigit() else None,
                'position': int(result['position']) if result['position'].isdigit() else None,
                'position_text': result['position'],
                'position_order': int(result['positionText']) if result['positionText'].isdigit() else 999,
                'points': float(result['points']),
                'laps': int(result['laps']),
                'time_milliseconds': time_ms,
                'fastest_lap': int(result.get('FastestLap', {}).get('lap', 0)) if result.get('FastestLap', {}).get('lap') else None,
                'fastest_lap_rank': int(result.get('FastestLap', {}).get('rank', 0)) if result.get('FastestLap', {}).get('rank') else None,
                'fastest_lap_time': result.get('FastestLap', {}).get('Time', {}).get('time'),
                'fastest_lap_speed': float(result.get('FastestLap', {}).get('AverageSpeed', {}).get('speed', 0)) if result.get('FastestLap', {}).get('AverageSpeed', {}).get('speed') else None,
                'status_id': self._get_status_id(result['status'])
            })
        
        return results
    
    def _time_to_milliseconds(self, time_str):
        """Convert time string like '1:34:50.616' to milliseconds"""
        try:
            parts = time_str.split(':')
            if len(parts) == 3:  # hours:minutes:seconds.milliseconds
                hours = int(parts[0])
                minutes = int(parts[1])
                seconds_parts = parts[2].split('.')
                seconds = int(seconds_parts[0])
                milliseconds = int(seconds_parts[1]) if len(seconds_parts) > 1 else 0
                
                total_ms = (hours * 3600 + minutes * 60 + seconds) * 1000 + milliseconds
                return total_ms
            elif len(parts) == 2:  # minutes:seconds.milliseconds
                minutes = int(parts[0])
                seconds_parts = parts[1].split('.')
                seconds = int(seconds_parts[0])
                milliseconds = int(seconds_parts[1]) if len(seconds_parts) > 1 else 0
                
                total_ms = (minutes * 60 + seconds) * 1000 + milliseconds
                return total_ms
        except:
            pass
        return None
    
    def _get_status_id(self, status_text):
        """Map status text to status ID"""
        status_map = {
            'Finished': 1, 'Disqualified': 2, 'Accident': 3, 'Collision': 4,
            'Engine': 5, 'Gearbox': 6, 'Transmission': 7, 'Clutch': 8,
            'Hydraulics': 9, 'Electrical': 10, '+1 Lap': 11, '+2 Laps': 12,
            '+3 Laps': 13, '+4 Laps': 14, '+5 Laps': 15, 'Spun off': 16,
            'Radiator': 17, 'Suspension': 18, 'Brakes': 19, 'Differential': 20
        }
        return status_map.get(status_text, 1)  # Default to 'Finished'
    
    def save_to_database(self, table_name, data):
        """Save data to SQLite database"""
        if not data:
            return
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get column names from first record
        columns = list(data[0].keys())
        placeholders = ','.join(['?' for _ in columns])
        column_names = ','.join(columns)
        
        # Prepare data for insertion
        values = []
        for record in data:
            values.append([record.get(col) for col in columns])
        
        try:
            cursor.executemany(
                f"INSERT OR REPLACE INTO {table_name} ({column_names}) VALUES ({placeholders})",
                values
            )
            conn.commit()
            print(f"Saved {len(data)} records to {table_name}")
        except sqlite3.Error as e:
            print(f"Error saving to {table_name}: {e}")
        finally:
            conn.close()

def main():
    """Main function to demonstrate usage"""
    client = ErgastClient()
    
    print("🚀 Starting F1 data collection...")
    
    # Fetch and save basic data
    circuits = client.get_circuits()
    client.save_to_database('circuits', circuits)
    
    drivers = client.get_drivers()  
    client.save_to_database('drivers', drivers)
    
    constructors = client.get_constructors()
    client.save_to_database('constructors', constructors)
    
    # Fetch seasons
    seasons = client.get_seasons(2020, 2024)  # Start with recent years
    client.save_to_database('seasons', seasons)
    
    # Fetch races for recent seasons
    for season in seasons[-2:]:  # Last 2 seasons for demo
        year = season['year']
        races = client.get_races_for_season(year)
        client.save_to_database('races', races)
        
        # Fetch race results for each race
        for race in races[:3]:  # First 3 races per season for demo
            results = client.get_race_results(year, race['round'])
            if results:
                client.save_to_database('race_results', results)
    
    print("Data collection complete!")

if __name__ == "__main__":
    main()