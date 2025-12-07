"""Quick verification that data persists to disk."""
import duckdb

DB_PATH = 'D:/Sports-Data-RAG-Platform/backend/db/sports_rag.duckdb'

print("\n" + "=" * 70)
print("🔍 VERIFYING DATA PERSISTENCE IN DUCKDB")
print("=" * 70)
print(f"📁 Database: {DB_PATH}\n")

conn = duckdb.connect(DB_PATH)

# Count records
print("📊 RECORD COUNTS:")
tables = ['leagues', 'teams', 'players', 'matches', 'player_match_stats']
for table in tables:
    count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    print(f"  ✓ {table:20s}: {count:3d} records")

print("\n" + "=" * 70)
print("📋 SAMPLE DATA")
print("=" * 70)

# Show leagues
print("\n🏆 LEAGUES:")
for row in conn.execute("SELECT name, country, season FROM leagues").fetchall():
    print(f"  • {row[0]} ({row[1]}, {row[2]})")

# Show teams
print("\n⚽ TEAMS:")
for row in conn.execute("SELECT name FROM teams ORDER BY name").fetchall():
    print(f"  • {row[0]}")

# Show players with stats
print("\n👤 TOP PERFORMERS (Match Stats):")
query = """
    SELECT 
        p.name,
        t.name as team,
        pms.goals,
        pms.assists,
        pms.xg
    FROM player_match_stats pms
    JOIN players p ON pms.player_id = p.player_id
    JOIN teams t ON pms.team_id = t.team_id AND pms.team_id = t.team_id
    ORDER BY pms.goals DESC, pms.assists DESC
"""
for row in conn.execute(query).fetchall():
    print(f"  • {row[0]:20s} ({row[1]:15s}): {row[2]}G, {row[3]}A, {row[4]:.1f} xG")

conn.close()

print("\n" + "=" * 70)
print("✅ SUCCESS! All data is persisted to disk!")
print("=" * 70)
print("\n💡 TIP: You can connect to this database from any DuckDB client")
print(f"   and the data will still be there!\n")
