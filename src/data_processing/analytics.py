"""
F1 Driver Performance Analytics Module
Advanced analytics for F1 driver and constructor performance
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime
import os

class F1Analytics:
    """Advanced F1 analytics class"""
    
    def __init__(self, db_path=None):
        if db_path is None:
            # Find the project root by looking for the data folder
            current_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = current_dir
            while project_root != '/':
                if os.path.exists(os.path.join(project_root, 'data', 'f1_database.db')):
                    break
                project_root = os.path.dirname(project_root)
            
            self.db_path = os.path.join(project_root, 'data', 'f1_database.db')
        else:
            self.db_path = db_path
        
        # Check if database exists
        if not os.path.exists(self.db_path):
            raise FileNotFoundError(f"Database not found at {self.db_path}")
        
        print(f"📁 Using database: {self.db_path}")
    
    def get_driver_performance_metrics(self):
        """Calculate comprehensive driver performance metrics"""
        
        conn = sqlite3.connect(self.db_path)
        
        query = """
        SELECT 
            d.driver_id,
            d.forename || ' ' || d.surname as driver_name,
            d.code,
            d.nationality,
            c.name as constructor,
            COUNT(rr.race_id) as races_completed,
            SUM(rr.points) as total_points,
            AVG(rr.points) as avg_points_per_race,
            AVG(rr.position) as avg_position,
            MIN(rr.position) as best_position,
            MAX(rr.position) as worst_position,
            SUM(CASE WHEN rr.position = 1 THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN rr.position <= 3 THEN 1 ELSE 0 END) as podiums,
            SUM(CASE WHEN rr.position <= 10 THEN 1 ELSE 0 END) as points_finishes,
            AVG(rr.grid) as avg_grid_position,
            (AVG(rr.grid) - AVG(rr.position)) as avg_grid_gain,
            COUNT(rr.fastest_lap) as fastest_laps
        FROM race_results rr
        JOIN drivers d ON rr.driver_id = d.driver_id
        JOIN constructors c ON rr.constructor_id = c.constructor_id
        WHERE rr.position IS NOT NULL
        GROUP BY d.driver_id, d.forename, d.surname, d.code, d.nationality, c.name
        ORDER BY total_points DESC
        """
        
        df = pd.read_sql_query(query, conn)
        
        # Calculate additional metrics
        df['podium_rate'] = df['podiums'] / df['races_completed'] * 100
        df['points_rate'] = df['points_finishes'] / df['races_completed'] * 100
        df['win_rate'] = df['wins'] / df['races_completed'] * 100
        
        # Performance consistency (standard deviation of positions)
        consistency_query = """
        SELECT 
            driver_id,
            ROUND(
                SQRT(
                    SUM((position - avg_pos) * (position - avg_pos)) / COUNT(*)
                ), 2
            ) as position_consistency
        FROM (
            SELECT 
                driver_id,
                position,
                AVG(position) OVER (PARTITION BY driver_id) as avg_pos
            FROM race_results 
            WHERE position IS NOT NULL
        )
        GROUP BY driver_id
        """
        
        consistency_df = pd.read_sql_query(consistency_query, conn)
        df = df.merge(consistency_df, on='driver_id', how='left')
        
        conn.close()
        return df
    
    def get_constructor_performance(self):
        """Analyze constructor/team performance"""
        
        conn = sqlite3.connect(self.db_path)
        
        query = """
        SELECT 
            c.constructor_id,
            c.name as constructor_name,
            c.nationality,
            COUNT(DISTINCT rr.driver_id) as drivers_used,
            COUNT(rr.race_id) as total_entries,
            SUM(rr.points) as total_points,
            AVG(rr.points) as avg_points_per_entry,
            AVG(rr.position) as avg_position,
            SUM(CASE WHEN rr.position = 1 THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN rr.position <= 3 THEN 1 ELSE 0 END) as podiums,
            SUM(CASE WHEN rr.position <= 10 THEN 1 ELSE 0 END) as points_finishes,
            COUNT(rr.fastest_lap) as fastest_laps
        FROM constructors c
        JOIN race_results rr ON c.constructor_id = rr.constructor_id
        WHERE rr.position IS NOT NULL
        GROUP BY c.constructor_id, c.name, c.nationality
        ORDER BY total_points DESC
        """
        
        df = pd.read_sql_query(query, conn)
        
        # Calculate rates
        df['podium_rate'] = df['podiums'] / df['total_entries'] * 100
        df['points_rate'] = df['points_finishes'] / df['total_entries'] * 100
        df['win_rate'] = df['wins'] / df['total_entries'] * 100
        
        conn.close()
        return df
    
    def get_race_analysis(self):
        """Analyze individual race performance"""
        
        conn = sqlite3.connect(self.db_path)
        
        query = """
        SELECT 
            r.race_id,
            r.name as race_name,
            r.date,
            c.name as circuit_name,
            c.country,
            COUNT(rr.driver_id) as participants,
            AVG(rr.position) as avg_finishing_position,
            d.forename || ' ' || d.surname as winner,
            winner_constructor.name as winning_constructor,
            rr.points as winner_points
        FROM races r
        JOIN circuits c ON r.circuit_id = c.circuit_id
        LEFT JOIN race_results rr ON r.race_id = rr.race_id
        LEFT JOIN (
            SELECT race_id, driver_id, constructor_id, points
            FROM race_results 
            WHERE position = 1
        ) winner_data ON r.race_id = winner_data.race_id
        LEFT JOIN drivers d ON winner_data.driver_id = d.driver_id
        LEFT JOIN constructors winner_constructor ON winner_data.constructor_id = winner_constructor.constructor_id
        GROUP BY r.race_id, r.name, r.date, c.name, c.country, d.forename, d.surname, winner_constructor.name, winner_data.points
        ORDER BY r.date DESC
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    
    def get_head_to_head_comparison(self, driver1_id, driver2_id):
        """Compare two drivers head-to-head"""
        
        conn = sqlite3.connect(self.db_path)
        
        # Get races where both drivers participated
        query = """
        SELECT 
            r.name as race_name,
            r.date,
            d1.forename || ' ' || d1.surname as driver1_name,
            rr1.position as driver1_position,
            rr1.points as driver1_points,
            d2.forename || ' ' || d2.surname as driver2_name,
            rr2.position as driver2_position,
            rr2.points as driver2_points,
            CASE 
                WHEN rr1.position < rr2.position THEN 1
                WHEN rr1.position > rr2.position THEN 2
                ELSE 0
            END as winner
        FROM races r
        JOIN race_results rr1 ON r.race_id = rr1.race_id AND rr1.driver_id = ?
        JOIN race_results rr2 ON r.race_id = rr2.race_id AND rr2.driver_id = ?
        JOIN drivers d1 ON rr1.driver_id = d1.driver_id
        JOIN drivers d2 ON rr2.driver_id = d2.driver_id
        WHERE rr1.position IS NOT NULL AND rr2.position IS NOT NULL
        ORDER BY r.date
        """
        
        df = pd.read_sql_query(query, conn, params=[driver1_id, driver2_id])
        
        # Calculate head-to-head stats
        if not df.empty:
            h2h_stats = {
                'total_races': len(df),
                'driver1_wins': len(df[df['winner'] == 1]),
                'driver2_wins': len(df[df['winner'] == 2]),
                'ties': len(df[df['winner'] == 0]),
                'driver1_avg_position': df['driver1_position'].mean(),
                'driver2_avg_position': df['driver2_position'].mean(),
                'driver1_total_points': df['driver1_points'].sum(),
                'driver2_total_points': df['driver2_points'].sum()
            }
        else:
            h2h_stats = {}
        
        conn.close()
        return df, h2h_stats
    
    def get_championship_progression(self):
        """Show championship points progression over races"""
        
        conn = sqlite3.connect(self.db_path)
        
        query = """
        SELECT 
            r.round,
            r.name as race_name,
            r.date,
            d.forename || ' ' || d.surname as driver_name,
            rr.points as race_points,
            SUM(rr.points) OVER (
                PARTITION BY d.driver_id 
                ORDER BY r.round 
                ROWS UNBOUNDED PRECEDING
            ) as cumulative_points
        FROM races r
        JOIN race_results rr ON r.race_id = rr.race_id
        JOIN drivers d ON rr.driver_id = d.driver_id
        ORDER BY r.round, cumulative_points DESC
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    
    def get_circuit_performance(self):
        """Analyze performance at different circuits"""
        
        conn = sqlite3.connect(self.db_path)
        
        query = """
        SELECT 
            c.name as circuit_name,
            c.country,
            COUNT(rr.race_id) as total_races,
            AVG(rr.position) as avg_position,
            d.forename || ' ' || d.surname as most_successful_driver,
            driver_wins.wins as driver_wins_at_circuit,
            constructor.name as most_successful_constructor,
            constructor_wins.wins as constructor_wins_at_circuit
        FROM circuits c
        JOIN races r ON c.circuit_id = r.circuit_id
        JOIN race_results rr ON r.race_id = rr.race_id
        LEFT JOIN (
            SELECT 
                r.circuit_id,
                rr.driver_id,
                COUNT(*) as wins,
                ROW_NUMBER() OVER (PARTITION BY r.circuit_id ORDER BY COUNT(*) DESC) as rn
            FROM races r
            JOIN race_results rr ON r.race_id = rr.race_id
            WHERE rr.position = 1
            GROUP BY r.circuit_id, rr.driver_id
        ) driver_wins ON c.circuit_id = driver_wins.circuit_id AND driver_wins.rn = 1
        LEFT JOIN drivers d ON driver_wins.driver_id = d.driver_id
        LEFT JOIN (
            SELECT 
                r.circuit_id,
                rr.constructor_id,
                COUNT(*) as wins,
                ROW_NUMBER() OVER (PARTITION BY r.circuit_id ORDER BY COUNT(*) DESC) as rn
            FROM races r
            JOIN race_results rr ON r.race_id = rr.race_id
            WHERE rr.position = 1
            GROUP BY r.circuit_id, rr.constructor_id
        ) constructor_wins ON c.circuit_id = constructor_wins.circuit_id AND constructor_wins.rn = 1
        LEFT JOIN constructors constructor ON constructor_wins.constructor_id = constructor.constructor_id
        GROUP BY c.circuit_id, c.name, c.country, d.forename, d.surname, driver_wins.wins, constructor.name, constructor_wins.wins
        ORDER BY total_races DESC
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df

def main():
    """Demo the analytics functionality"""
    
    analytics = F1Analytics()
    
    print("🏁 F1 Advanced Analytics Demo")
    print("=" * 60)
    
    # Driver performance
    print("\n📊 Driver Performance Metrics:")
    driver_perf = analytics.get_driver_performance_metrics()
    print(driver_perf[['driver_name', 'total_points', 'avg_position', 'podium_rate', 'position_consistency']].head())
    
    # Constructor performance  
    print("\n🏭 Constructor Performance:")
    constructor_perf = analytics.get_constructor_performance()
    print(constructor_perf[['constructor_name', 'total_points', 'avg_position', 'podium_rate']].head())
    
    # Championship progression
    print("\n🏆 Championship Progression:")
    championship = analytics.get_championship_progression()
    print(championship.head(15))
    
    # Circuit analysis
    print("\n🏁 Circuit Performance:")
    circuit_perf = analytics.get_circuit_performance()
    print(circuit_perf.head())
    
    print("\n✅ Analytics complete! Ready for dashboard creation.")

if __name__ == "__main__":
    main()