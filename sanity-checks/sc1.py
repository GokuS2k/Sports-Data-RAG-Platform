import duckdb
con = duckdb.connect("duckdb/fit.db", read_only=True)
print("Tables:", [r[0] for r in con.execute("SHOW TABLES").fetchall()])
print("Feature order:", con.execute("SELECT * FROM feature_order ORDER BY ord").fetchall())
con.close()
