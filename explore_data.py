"""
Simple data exploration script to verify F1 database is working
"""

import sqlite3
import pandas as pd

def explore_f1_data():
    """Explore the F1 database and show basic statistics"""
    
    conn = sqlite3.connect('data/f1_database.db')
    
    print("🏁 F1 Data Explorer")
    print("=" * 50)
    
    # Show table contents
    tables = ['circuits', 'drivers', 'constructors', 'races', 'race_results']
    
    for table in tables:
        query = f"SELECT COUNT(*) FROM {table}"
        count = pd.read_sql_query(query, conn).iloc[0, 0]
        print(f"📊 {table.capitalize()}: {count} records")
    
    print("\n" + "=" * 50)
    
    # Show top drivers by points
    query = """
    SELECT 
        d.forename || ' ' || d.surname as driver_name,
        SUM(rr.points) as total_points,
        COUNT(rr.race_id) as races_completed,
        AVG(rr.position) as avg_position
    FROM race_results rr
    JOIN drivers d ON rr.driver_id = d.driver_id
    GROUP BY rr.driver_id, d.forename, d.surname
    ORDER BY total_points DESC
    LIMIT 10
    """
    
    df = pd.read_sql_query(query, conn)
    print("🏆 Driver Championship Standings:")
    print(df.to_string(index=False))
    
    print("\n" + "=" * 50)
    
    # Show constructor standings
    query = """
    SELECT 
        c.name as constructor_name,
        SUM(rr.points) as total_points,
        COUNT(DISTINCT rr.driver_id) as drivers_count,
        AVG(rr.position) as avg_position
    FROM race_results rr
    JOIN constructors c ON rr.constructor_id = c.constructor_id
    GROUP BY rr.constructor_id, c.name
    ORDER BY total_points DESC
    LIMIT 10
    """
    
    df = pd.read_sql_query(query, conn)
    print("🏭 Constructor Championship Standings:")
    print(df.to_string(index=False))
    
    print("\n" + "=" * 50)
    
    # Show race winners
    query = """
    SELECT 
        r.name as race_name,
        d.forename || ' ' || d.surname as winner,
        c.name as constructor,
        rr.points
    FROM race_results rr
    JOIN races r ON rr.race_id = r.race_id
    JOIN drivers d ON rr.driver_id = d.driver_id
    JOIN constructors c ON rr.constructor_id = c.constructor_id
    WHERE rr.position = 1
    ORDER BY r.round
    """
    
    df = pd.read_sql_query(query, conn)
    print("🥇 Race Winners:")
    print(df.to_string(index=False))
    
    conn.close()
    print("\n✅ Data exploration complete!")
    print("🚀 Ready to build analytics and dashboard!")

if __name__ == "__main__":
    explore_f1_data()