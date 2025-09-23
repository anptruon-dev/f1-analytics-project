"""
Create sample F1 data for development when Ergast API is unavailable
This creates realistic sample data to demonstrate the project functionality
"""

import sqlite3
import random
from datetime import datetime, timedelta

def create_sample_data():
    """Create sample F1 data for project development"""
    
    conn = sqlite3.connect('data/f1_database.db')
    cursor = conn.cursor()
    
    print("Creating sample F1 data...")
    
    # Sample circuits - using individual INSERT statements to avoid datatype issues
    circuits_data = [
        (1, 'bahrain', 'Bahrain International Circuit', 'Sakhir', 'Bahrain', 26.0325, 50.5106, 7, 'http://en.wikipedia.org/wiki/Bahrain_International_Circuit'),
        (2, 'jeddah', 'Jeddah Corniche Circuit', 'Jeddah', 'Saudi Arabia', 21.3099, 39.1044, 15, 'http://en.wikipedia.org/wiki/Jeddah_Street_Circuit'),
        (3, 'albert_park', 'Albert Park Grand Prix Circuit', 'Melbourne', 'Australia', -37.8497, 144.968, 12, 'http://en.wikipedia.org/wiki/Melbourne_Grand_Prix_Circuit'),
        (4, 'imola', 'Autodromo Enzo e Dino Ferrari', 'Imola', 'Italy', 44.3439, 11.7167, 37, 'http://en.wikipedia.org/wiki/Autodromo_Enzo_e_Dino_Ferrari'),
        (5, 'miami', 'Miami International Autodrome', 'Miami', 'USA', 25.7585, -80.2389, 3, 'http://en.wikipedia.org/wiki/Miami_International_Autodrome'),
        (6, 'spain', 'Circuit de Barcelona-Catalunya', 'Montmeló', 'Spain', 41.57, 2.26111, 109, 'http://en.wikipedia.org/wiki/Circuit_de_Barcelona-Catalunya'),
        (7, 'monaco', 'Circuit de Monaco', 'Monaco', 'Monaco', 43.7347, 7.42056, 7, 'http://en.wikipedia.org/wiki/Circuit_de_Monaco'),
        (8, 'baku', 'Baku City Circuit', 'Baku', 'Azerbaijan', 40.3725, 49.8533, -1, 'http://en.wikipedia.org/wiki/Baku_City_Circuit'),
        (9, 'canada', 'Circuit Gilles Villeneuve', 'Montreal', 'Canada', 45.5, -73.5228, 13, 'http://en.wikipedia.org/wiki/Circuit_Gilles_Villeneuve'),
        (10, 'silverstone', 'Silverstone Circuit', 'Silverstone', 'UK', 52.0786, -1.01694, 153, 'http://en.wikipedia.org/wiki/Silverstone_Circuit')
    ]
    
    cursor.executemany(
        "INSERT OR REPLACE INTO circuits (circuit_id, circuit_ref, name, location, country, lat, lng, alt, url) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        circuits_data
    )
    
    # Sample drivers
    drivers_data = [
        (1, 'max_verstappen', 1, 'VER', 'Max', 'Verstappen', '1997-09-30', 'Dutch', 'http://en.wikipedia.org/wiki/Max_Verstappen'),
        (2, 'leclerc', 16, 'LEC', 'Charles', 'Leclerc', '1997-10-16', 'Monégasque', 'http://en.wikipedia.org/wiki/Charles_Leclerc'),
        (3, 'sainz', 55, 'SAI', 'Carlos', 'Sainz Jr.', '1994-09-01', 'Spanish', 'http://en.wikipedia.org/wiki/Carlos_Sainz_Jr.'),
        (4, 'perez', 11, 'PER', 'Sergio', 'Pérez', '1990-01-26', 'Mexican', 'http://en.wikipedia.org/wiki/Sergio_P%C3%A9rez'),
        (5, 'russell', 63, 'RUS', 'George', 'Russell', '1998-02-15', 'British', 'http://en.wikipedia.org/wiki/George_Russell_%28racing_driver%29'),
        (6, 'hamilton', 44, 'HAM', 'Lewis', 'Hamilton', '1985-01-07', 'British', 'http://en.wikipedia.org/wiki/Lewis_Hamilton'),
        (7, 'norris', 4, 'NOR', 'Lando', 'Norris', '1999-11-13', 'British', 'http://en.wikipedia.org/wiki/Lando_Norris'),
        (8, 'piastri', 81, 'PIA', 'Oscar', 'Piastri', '2001-04-06', 'Australian', 'http://en.wikipedia.org/wiki/Oscar_Piastri'),
        (9, 'alonso', 14, 'ALO', 'Fernando', 'Alonso', '1981-07-29', 'Spanish', 'http://en.wikipedia.org/wiki/Fernando_Alonso'),
        (10, 'ocon', 31, 'OCO', 'Esteban', 'Ocon', '1996-09-17', 'French', 'http://en.wikipedia.org/wiki/Esteban_Ocon')
    ]
    
    cursor.executemany(
        "INSERT OR REPLACE INTO drivers (driver_id, driver_ref, number, code, forename, surname, dob, nationality, url) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        drivers_data
    )
    
    # Sample constructors
    constructors_data = [
        (1, 'red_bull', 'Red Bull Racing Honda RBPT', 'Austrian', 'http://en.wikipedia.org/wiki/Red_Bull_Racing'),
        (2, 'ferrari', 'Ferrari', 'Italian', 'http://en.wikipedia.org/wiki/Scuderia_Ferrari'),
        (3, 'mercedes', 'Mercedes', 'German', 'http://en.wikipedia.org/wiki/Mercedes-Benz_in_Formula_One'),
        (4, 'mclaren', 'McLaren Mercedes', 'British', 'http://en.wikipedia.org/wiki/McLaren'),
        (5, 'aston_martin', 'Aston Martin Aramco Mercedes', 'British', 'http://en.wikipedia.org/wiki/Aston_Martin_in_Formula_One'),
        (6, 'alpine', 'Alpine Renault', 'French', 'http://en.wikipedia.org/wiki/Alpine_F1_Team'),
        (7, 'williams', 'Williams Mercedes', 'British', 'http://en.wikipedia.org/wiki/Williams_Grand_Prix_Engineering'),
        (8, 'alphatauri', 'AlphaTauri Honda RBPT', 'Italian', 'http://en.wikipedia.org/wiki/Scuderia_AlphaTauri'),
        (9, 'alfa', 'Alfa Romeo Ferrari', 'Swiss', 'http://en.wikipedia.org/wiki/Alfa_Romeo_in_Formula_One'),
        (10, 'haas', 'Haas Ferrari', 'American', 'http://en.wikipedia.org/wiki/Haas_F1_Team')
    ]
    
    cursor.executemany(
        "INSERT OR REPLACE INTO constructors (constructor_id, constructor_ref, name, nationality, url) VALUES (?, ?, ?, ?, ?)",
        constructors_data
    )
    
    # Sample seasons
    seasons_data = [(2023, 'http://en.wikipedia.org/wiki/2023_Formula_One_World_Championship'),
                   (2024, 'http://en.wikipedia.org/wiki/2024_Formula_One_World_Championship')]
    
    cursor.executemany("INSERT OR REPLACE INTO seasons (year, url) VALUES (?, ?)", seasons_data)
    
    # Sample races for 2024
    races_data = [
        (1, 2024, 1, 1, 'Bahrain Grand Prix', '2024-03-02', '15:00:00', 'https://en.wikipedia.org/wiki/2024_Bahrain_Grand_Prix'),
        (2, 2024, 2, 2, 'Saudi Arabian Grand Prix', '2024-03-09', '18:00:00', 'https://en.wikipedia.org/wiki/2024_Saudi_Arabian_Grand_Prix'),
        (3, 2024, 3, 3, 'Australian Grand Prix', '2024-03-24', '05:00:00', 'https://en.wikipedia.org/wiki/2024_Australian_Grand_Prix'),
        (4, 2024, 4, 4, 'Emilia Romagna Grand Prix', '2024-05-19', '13:00:00', 'https://en.wikipedia.org/wiki/2024_Emilia_Romagna_Grand_Prix'),
        (5, 2024, 5, 5, 'Miami Grand Prix', '2024-05-05', '20:30:00', 'https://en.wikipedia.org/wiki/2024_Miami_Grand_Prix')
    ]
    
    cursor.executemany(
        "INSERT OR REPLACE INTO races (race_id, year, round, circuit_id, name, date, time, url) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        races_data
    )
    
    # Create sample race results
    print("Generating race results...")
    
    # Driver-constructor pairings (realistic for 2024) - using IDs instead of strings
    driver_teams = {
        1: 1,   # max_verstappen: red_bull
        4: 1,   # perez: red_bull
        2: 2,   # leclerc: ferrari 
        3: 2,   # sainz: ferrari
        6: 3,   # hamilton: mercedes
        5: 3,   # russell: mercedes
        7: 4,   # norris: mclaren
        8: 4,   # piastri: mclaren
        9: 5,   # alonso: aston_martin
        10: 6   # ocon: alpine
    }
    
    # Generate results for each race
    for race_id, year, round_num, circuit_id, race_name, date, time, url in races_data:
        drivers_list = list(driver_teams.keys())
        random.shuffle(drivers_list)  # Random finishing order
        
        for position, driver_id in enumerate(drivers_list, 1):
            constructor_id = driver_teams[driver_id]
            
            # Simulate realistic results
            points_system = [25, 18, 15, 12, 10, 8, 6, 4, 2, 1] + [0] * 10
            points = points_system[position-1] if position <= len(points_system) else 0
            
            # Simulate race time (winner gets actual time, others get gaps)
            if position == 1:
                race_time_ms = random.randint(5400000, 6000000)  # 1.5-1.67 hours in ms
            else:
                race_time_ms = None  # Others would have gap times
            
            result_data = (
                race_id, driver_id, constructor_id, 
                random.randint(1, 20),  # car number
                random.randint(1, 10),   # grid position
                position,                # finishing position
                str(position),          # position text
                position,               # position order
                points,                 # points scored
                random.randint(50, 70), # laps completed
                race_time_ms,           # race time
                random.randint(10, 60) if random.random() > 0.3 else None,  # fastest lap
                random.randint(1, 10) if random.random() > 0.5 else None,   # fastest lap rank
                f"{random.randint(1, 2)}:{random.randint(10, 30)}.{random.randint(100, 999)}" if random.random() > 0.3 else None,  # fastest lap time
                random.uniform(200, 350) if random.random() > 0.3 else None,  # fastest lap speed
                1  # status (finished)
            )
            
            cursor.execute("""
                INSERT OR REPLACE INTO race_results 
                (race_id, driver_id, constructor_id, number, grid, position, position_text, 
                 position_order, points, laps, time_milliseconds, fastest_lap, fastest_lap_rank, 
                 fastest_lap_time, fastest_lap_speed, status_id) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, result_data)
    
    conn.commit()
    conn.close()
    
    print("Sample data created successfully!")
    print("Data includes:")
    print("   - 10 circuits (Bahrain, Jeddah, Melbourne, etc.)")
    print("   - 10 drivers (Verstappen, Leclerc, Hamilton, etc.)")
    print("   - 10 constructors (Red Bull, Ferrari, Mercedes, etc.)")
    print("   - 5 races from 2024 season")
    print("   - Race results for all drivers")
    
    # Show some statistics
    conn = sqlite3.connect('data/f1_database.db')
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM race_results")
    result_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT name, COUNT(*) as races FROM races JOIN race_results ON races.race_id = race_results.race_id GROUP BY name")
    race_stats = cursor.fetchall()
    
    print(f"\nDatabase Statistics:")
    print(f"   - Total race results: {result_count}")
    print(f"   - Races with data: {len(race_stats)}")
    
    conn.close()

if __name__ == "__main__":
    create_sample_data()