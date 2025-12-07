"""
Simple script to display all data in a readable format.
Run this anytime you want to see what's in your database!
"""

import duckdb
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "db" / "sports_rag.duckdb"

def print_separator(title=""):
    print("\n" + "=" * 80)
    if title:
        print(f"  {title}")
        print("=" * 80)

def view_all_data():
    """Display all data from the database in a readable format."""
    conn = duckdb.connect(str(DB_PATH))
    
    print_separator("🗄️  SPORTS RAG DATABASE VIEWER")
    print(f"📁 Database: {DB_PATH}")
    
    # Show all tables
    print_separator("📋 TABLES")
    tables = conn.execute("SHOW TABLES").fetchall()
    for table in tables:
        count = conn.execute(f"SELECT COUNT(*) FROM {table[0]}").fetchone()[0]
        print(f"  ✓ {table[0]:25s} ({count} rows)")
    
    # Leagues
    print_separator("🏆 LEAGUES")
    result = conn.execute("SELECT * FROM leagues").fetchall()
    if result:
        print(f"  {'ID':<5} {'Name':<25} {'Country':<15} {'Season':<10}")
        print("  " + "-" * 60)
        for row in result:
            print(f"  {row[0]:<5} {row[1]:<25} {row[2]:<15} {row[3]:<10}")
    else:
        print("  (No data)")
    
    # Teams
    print_separator("⚽ TEAMS")
    result = conn.execute("SELECT * FROM teams ORDER BY name").fetchall()
    if result:
        print(f"  {'ID':<5} {'Name':<25} {'League ID':<10} {'Season':<10}")
        print("  " + "-" * 60)
        for row in result:
            print(f"  {row[0]:<5} {row[1]:<25} {row[2]:<10} {row[3]:<10}")
    else:
        print("  (No data)")
    
    # Players
    print_separator("👤 PLAYERS")
    result = conn.execute("""
        SELECT p.player_id, p.name, p.position, t.name as team_name, p.season
        FROM players p
        LEFT JOIN teams t ON p.team_id = t.team_id AND p.season = t.season
        ORDER BY p.name
    """).fetchall()
    if result:
        print(f"  {'ID':<5} {'Name':<25} {'Position':<15} {'Team':<20} {'Season':<10}")
        print("  " + "-" * 80)
        for row in result:
            print(f"  {row[0]:<5} {row[1]:<25} {row[2]:<15} {row[3] or 'N/A':<20} {row[4]:<10}")
    else:
        print("  (No data)")
    
    # Matches
    print_separator("🏟️  MATCHES")
    result = conn.execute("""
        SELECT 
            m.match_id,
            m.match_date,
            h.name as home_team,
            a.name as away_team,
            m.season
        FROM matches m
        LEFT JOIN teams h ON m.home_team_id = h.team_id AND m.season = h.season
        LEFT JOIN teams a ON m.away_team_id = a.team_id AND m.season = a.season
        ORDER BY m.match_date DESC
    """).fetchall()
    if result:
        print(f"  {'ID':<5} {'Date':<12} {'Home Team':<20} {'Away Team':<20} {'Season':<10}")
        print("  " + "-" * 80)
        for row in result:
            print(f"  {row[0]:<5} {str(row[1]):<12} {row[2] or 'N/A':<20} {row[3] or 'N/A':<20} {row[4]:<10}")
    else:
        print("  (No data)")
    
    # Player Match Stats
    print_separator("📊 PLAYER MATCH STATISTICS")
    result = conn.execute("""
        SELECT 
            p.name,
            t.name as team,
            pms.minutes,
            pms.goals,
            pms.assists,
            pms.shots,
            pms.xg,
            pms.passes_completed,
            pms.passes_attempted,
            pms.progressive_passes
        FROM player_match_stats pms
        JOIN players p ON pms.player_id = p.player_id
        JOIN teams t ON pms.team_id = t.team_id
        ORDER BY pms.goals DESC, pms.assists DESC
    """).fetchall()
    if result:
        print(f"\n  {'Player':<25} {'Team':<20} {'Min':<5} {'G':<3} {'A':<3} {'Shots':<6} {'xG':<6} {'Passes':<12} {'Prog':<5}")
        print("  " + "-" * 95)
        for row in result:
            passes = f"{row[7]}/{row[8]}"
            print(f"  {row[0]:<25} {row[1]:<20} {row[2]:<5} {row[3]:<3} {row[4]:<3} {row[5]:<6} {row[6]:<6.1f} {passes:<12} {row[9]:<5}")
    else:
        print("  (No data)")
    
    print_separator()
    print("✅ Database viewer complete!\n")
    
    conn.close()

if __name__ == "__main__":
    view_all_data()
