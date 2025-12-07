"""
Test script to verify DuckDB persistence to disk.
This script will:
1. Insert sample data into the database
2. Query the data to verify it was inserted
3. Close the connection
4. Reopen the connection and verify data persists
"""

from db import get_db_session

def insert_sample_data():
    """Insert sample sports data into the database."""
    print("=" * 60)
    print("STEP 1: Inserting sample data...")
    print("=" * 60)
    
    with get_db_session() as conn:
        # Insert a league
        conn.execute("""
            INSERT INTO leagues (league_id, name, country, season)
            VALUES (1, 'Premier League', 'England', '2023-24')
        """)
        print("✓ Inserted league: Premier League (England, 2023-24)")
        
        # Insert teams
        conn.execute("""
            INSERT INTO teams (team_id, name, league_id, season)
            VALUES 
                (1, 'Manchester City', 1, '2023-24'),
                (2, 'Arsenal', 1, '2023-24'),
                (3, 'Liverpool', 1, '2023-24')
        """)
        print("✓ Inserted 3 teams")
        
        # Insert players
        conn.execute("""
            INSERT INTO players (player_id, name, team_id, season, position)
            VALUES 
                (1, 'Erling Haaland', 1, '2023-24', 'Forward'),
                (2, 'Kevin De Bruyne', 1, '2023-24', 'Midfielder'),
                (3, 'Bukayo Saka', 2, '2023-24', 'Forward'),
                (4, 'Mohamed Salah', 3, '2023-24', 'Forward')
        """)
        print("✓ Inserted 4 players")
        
        # Insert a match
        conn.execute("""
            INSERT INTO matches (match_id, league_id, season, home_team_id, away_team_id, match_date)
            VALUES (1, 1, '2023-24', 1, 2, '2024-03-31')
        """)
        print("✓ Inserted 1 match: Man City vs Arsenal")
        
        # Insert player stats
        conn.execute("""
            INSERT INTO player_match_stats 
            (match_id, player_id, team_id, minutes, goals, assists, shots, xg, 
             passes_completed, passes_attempted, progressive_passes)
            VALUES 
                (1, 1, 1, 90, 2, 0, 5, 1.8, 25, 30, 3),
                (1, 2, 1, 90, 0, 2, 2, 0.3, 65, 72, 12),
                (1, 3, 2, 90, 1, 1, 4, 1.2, 45, 52, 8)
        """)
        print("✓ Inserted player match stats for 3 players")
    
    print("\n✅ All data inserted successfully and committed to disk!\n")


def query_data():
    """Query and display the inserted data."""
    print("=" * 60)
    print("STEP 2: Querying data from database...")
    print("=" * 60)
    
    with get_db_session() as conn:
        # Query leagues
        print("\n📊 LEAGUES:")
        leagues = conn.execute("SELECT * FROM leagues").fetchall()
        for league in leagues:
            print(f"  - {league[1]} ({league[2]}, {league[3]})")
        
        # Query teams
        print("\n⚽ TEAMS:")
        teams = conn.execute("SELECT * FROM teams").fetchall()
        for team in teams:
            print(f"  - {team[1]}")
        
        # Query players with their teams
        print("\n👤 PLAYERS:")
        players = conn.execute("""
            SELECT p.name, p.position, t.name as team_name
            FROM players p
            JOIN teams t ON p.team_id = t.team_id AND p.season = t.season
        """).fetchall()
        for player in players:
            print(f"  - {player[0]} ({player[1]}) - {player[2]}")
        
        # Query match stats
        print("\n📈 MATCH STATS (Man City vs Arsenal):")
        stats = conn.execute("""
            SELECT p.name, pms.goals, pms.assists, pms.shots, pms.xg, 
                   pms.passes_completed, pms.passes_attempted
            FROM player_match_stats pms
            JOIN players p ON pms.player_id = p.player_id
            ORDER BY pms.goals DESC, pms.assists DESC
        """).fetchall()
        for stat in stats:
            print(f"  - {stat[0]}: {stat[1]}G, {stat[2]}A, {stat[3]} shots, "
                  f"{stat[4]:.1f} xG, {stat[5]}/{stat[6]} passes")
    
    print("\n✅ Data queried successfully!\n")


def verify_persistence():
    """Verify that data persists after closing and reopening connection."""
    print("=" * 60)
    print("STEP 3: Verifying data persistence...")
    print("=" * 60)
    print("Opening a NEW connection to verify data was saved to disk...\n")
    
    with get_db_session() as conn:
        # Count records in each table
        league_count = conn.execute("SELECT COUNT(*) FROM leagues").fetchone()[0]
        team_count = conn.execute("SELECT COUNT(*) FROM teams").fetchone()[0]
        player_count = conn.execute("SELECT COUNT(*) FROM players").fetchone()[0]
        match_count = conn.execute("SELECT COUNT(*) FROM matches").fetchone()[0]
        stats_count = conn.execute("SELECT COUNT(*) FROM player_match_stats").fetchone()[0]
        
        print(f"📊 Record counts:")
        print(f"  - Leagues: {league_count}")
        print(f"  - Teams: {team_count}")
        print(f"  - Players: {player_count}")
        print(f"  - Matches: {match_count}")
        print(f"  - Player Stats: {stats_count}")
        
        if all([league_count > 0, team_count > 0, player_count > 0, 
                match_count > 0, stats_count > 0]):
            print("\n✅ SUCCESS! All data persisted to disk correctly!")
            print(f"📁 Database file: D:\\Sports-Data-RAG-Platform\\backend\\db\\sports_rag.duckdb")
        else:
            print("\n❌ ERROR: Some data was not persisted!")
    
    print()


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("🧪 TESTING DUCKDB PERSISTENCE")
    print("=" * 60 + "\n")
    
    # Run the test
    insert_sample_data()
    query_data()
    verify_persistence()
    
    print("=" * 60)
    print("🎉 TEST COMPLETE!")
    print("=" * 60)
    print("\nYou can now:")
    print("1. Close this script")
    print("2. Open a DuckDB client")
    print("3. Connect to: D:\\Sports-Data-RAG-Platform\\backend\\db\\sports_rag.duckdb")
    print("4. Query the data - it will still be there!")
    print()
