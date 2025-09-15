import streamlit as st, duckdb
st.set_page_config(page_title="AI Team Fit — MVP", layout="wide")

con = duckdb.connect('duckdb/fit.db', read_only=True)
teams = con.execute("SELECT COUNT(*) FROM teams").fetchone()[0]
players = con.execute("SELECT COUNT(*) FROM players").fetchone()[0]

st.title("AI Team Fit — MVP")
st.write(f"Teams in DB: **{teams}**  |  Players in DB: **{players}**")
st.info("Ingestion not run yet. Next step: FBref ingest for one league/season.")
