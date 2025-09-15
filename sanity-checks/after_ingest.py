import duckdb
con = duckdb.connect('duckdb/fit.db', read_only=True)

print('Teams:', con.execute('SELECT COUNT(*) FROM teams').fetchone()[0])
print('Players:', con.execute('SELECT COUNT(*) FROM players').fetchone()[0])
print('Player season rows:', con.execute('SELECT COUNT(*) FROM player_season_stats').fetchone()[0])

print('\nSample player stats:')
print(con.execute("""
SELECT player_id, minutes,
       progressive_passes_per90, progressive_carries_per90,
       pressures_per90, tackles_interceptions_per90,
       aerials_won_pct
FROM player_season_stats
ORDER BY minutes DESC LIMIT 5
""").fetch_df())

print('\nTeam style sample:')
print(con.execute("""
SELECT team_id, minutes,
       progressive_passes_per90, progressive_carries_per90,
       pressures_att3rd_per90, aerials_win_pct
FROM team_season_stats
ORDER BY minutes DESC LIMIT 5
""").fetch_df())
