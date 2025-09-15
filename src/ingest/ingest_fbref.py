import time, re, json
from typing import Dict, List
import duckdb
import numpy as np
import pandas as pd
import requests_cache
from bs4 import BeautifulSoup, Comment

# -----------------------------
# Config: Premier League 2023–24
# -----------------------------
COMP_ID = 9
SEASON_SLUG = "2023-2024"
LEAGUE = "Premier League"
SEASON = "2023-24"  # how we'll store in DB

BASE = "https://fbref.com"
SESS = requests_cache.CachedSession(".cache_fbref", expire_after=60*60*24)

TABLE_ENDPOINTS = {
    "standard": f"/en/comps/{COMP_ID}/{SEASON_SLUG}/stats/players/{COMP_ID}-{SEASON_SLUG}-players-{LEAGUE.replace(' ','-')}-Stats",
    "passing":  f"/en/comps/{COMP_ID}/{SEASON_SLUG}/passing/players/{COMP_ID}-{SEASON_SLUG}-players-{LEAGUE.replace(' ','-')}-Passing",
    "possession": f"/en/comps/{COMP_ID}/{SEASON_SLUG}/possession/players/{COMP_ID}-{SEASON_SLUG}-players-{LEAGUE.replace(' ','-')}-Possession",
    "defense":  f"/en/comps/{COMP_ID}/{SEASON_SLUG}/defense/players/{COMP_ID}-{SEASON_SLUG}-players-{LEAGUE.replace(' ','-')}-Defense"
}

NUMERIC_FIX = re.compile(r"[^0-9\.\-]")

def _unwrap_comments(html: str) -> str:
    """FBref sometimes wraps tables in HTML comments. This reveals them."""
    soup = BeautifulSoup(html, "html.parser")
    # Extract comment blocks that contain tables and append back
    for c in soup.find_all(string=lambda text: isinstance(text, Comment)):
        if "<table" in c or "<thead" in c:
            new = BeautifulSoup(c, "html.parser")
            c.replace_with(new)
    return str(soup)

def _read_table(url: str) -> pd.DataFrame:
    resp = SESS.get(url)
    resp.raise_for_status()
    soup_html = _unwrap_comments(resp.text)
    # FBref pages might include multiple tables; the first big player table is what we want
    dfs = pd.read_html(soup_html, flavor="lxml")
    # Find the table that has a "Player" column
    df = next((d for d in dfs if "Player" in d.columns), None)
    if df is None:
        raise RuntimeError(f"No player table found at {url}")
    # Drop header rows repeated in body
    df = df[df["Player"] != "Player"].copy()
    return df

def _clean_cols(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [re.sub(r"[^a-z0-9_]+", "_", c.lower()).strip("_") for c in df.columns]
    return df

def _to_numeric(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c].astype(str).str.replace(",", "", regex=False).str.replace("%", "", regex=False), errors="coerce")
    return df

def fetch_league_player_frames() -> Dict[str, pd.DataFrame]:
    frames = {}
    for key, endpoint in TABLE_ENDPOINTS.items():
        url = BASE + endpoint
        print(f"Fetching {key}: {url}")
        df = _read_table(url)
        df = _clean_cols(df)
        frames[key] = df
        time.sleep(1.5)  # be nice
    return frames

def build_player_master(frames: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    std = frames["standard"].copy()
    pas = frames["passing"].copy()
    pos = frames["possession"].copy()
    dfn = frames["defense"].copy()

    # Keep useful columns
    # standard has minutes ("min"), 90s ("90s"), position ("pos"), team ("squad")
    keep_std = ["player","squad","pos","age","nationality","90s","min"]
    std = std[[c for c in keep_std if c in std.columns]].copy()
    std = _to_numeric(std, ["90s","min"])

    # Passing: progressive passes (prgp), through balls completed (tb? sometimes 't_b' doesn't exist; keep prgp)
    keep_pas = ["player","squad","prgp"]  # PrgP
    pas = pas[[c for c in keep_pas if c in pas.columns]].copy()
    pas = _to_numeric(pas, [c for c in keep_pas if c not in ["player","squad"]])

    # Possession: progressive carries (prgc), carries into final third (carries_into_final_third? often 'cpa' is crosses into penalty area; we use prgc + carries_into_final_third if present)
    # On FBref, 'Carries into final third' header is 'Carries', column 'Carries into final third' -> often short code 'CrsFT' doesn't exist; use 'carries_into_final_third' if present
    poss_cols = ["player","squad","prgc","carries_into_final_third","att_dribbles","succ"]  # prgc=PrgC, dribbles completed uses succ from dribbles section
    poss_cols = [c for c in poss_cols if c in pos.columns] + ["player","squad"]
    poss_cols = list(dict.fromkeys(poss_cols))  # unique, preserve order
    pos = pos[[c for c in poss_cols if c in pos.columns]].copy()
    numeric_pos = [c for c in poss_cols if c not in ["player","squad"]]
    pos = _to_numeric(pos, numeric_pos)

    # Defense: pressures (pressures), tackles+interceptions (tackles_interceptions = 'tkl_int'), aerials (aer_won%, 'aer_won_%' not raw; we can compute pct=won/(won+lost))
    keep_def = ["player","squad","pressures","tkl_int","aer_won","aer_lost"]
    dfn = dfn[[c for c in keep_def if c in dfn.columns]].copy()
    dfn = _to_numeric(dfn, [c for c in keep_def if c not in ["player","squad"]])

    # Merge on player+squad
    m = std.merge(pas, on=["player","squad"], how="left")\
           .merge(pos, on=["player","squad"], how="left")\
           .merge(dfn, on=["player","squad"], how="left")

    # Fill NaNs with 0 for count-like stats
    for c in ["prgp","prgc","carries_into_final_third","att_dribbles","succ","pressures","tkl_int","aer_won","aer_lost"]:
        if c in m.columns:
            m[c] = m[c].fillna(0)

    # Per-90 using 90s column (safer than minutes across tables)
    m["nineties"] = m["90s"].replace(0, np.nan)
    def per90(x, ninety):
        return np.where(ninety>0, x / ninety, 0.0)

    # Build our MVP features
    out = pd.DataFrame()
    out["player_name"] = m["player"]
    out["team_name"]   = m["squad"]
    out["pos"]         = m.get("pos", "NA")
    out["minutes"]     = m["min"].fillna(0)

    out["progressive_passes_per90"] = per90(m.get("prgp", 0), m["nineties"])
    out["progressive_carries_per90"] = per90(m.get("prgc", 0), m["nineties"])
    # Dribbles completed: 'succ' from possession dribbles section
    out["dribbles_completed_per90"] = per90(m.get("succ", 0), m["nineties"])
    # Final third entries via carries if present
    out["carries_into_final_third_per90"] = per90(m.get("carries_into_final_third", 0), m["nineties"])
    # Shots/xG/xA are in 'shooting' and 'passing' creation tables; for MVP we skip and keep placeholders 0
    out["shots_per90"] = 0.0
    out["xg_per90"] = 0.0
    out["xa_per90"] = 0.0
    # Pressures + tackles+interceptions
    out["pressures_per90"] = per90(m.get("pressures", 0), m["nineties"])
    out["tackles_interceptions_per90"] = per90(m.get("tkl_int", 0), m["nineties"])
    # Aerials win %
    aer_won = m.get("aer_won", pd.Series(0, index=m.index))
    aer_lost = m.get("aer_lost", pd.Series(0, index=m.index))
    aer_total = aer_won + aer_lost
    out["aerials_won_pct"] = np.where(aer_total>0, (aer_won / aer_total) * 100.0, np.nan)

    # Crosses into box & through balls are league-rare columns; keep as nulls for now
    out["crosses_into_box_per90"] = 0.0
    out["through_balls_completed_per90"] = 0.0

    # Clean names / ids
    out["player_id"] = out["player_name"].str.lower().str.replace(r"[^a-z0-9]+","_", regex=True) + "_" + SEASON.replace("-","")
    out["team_id"]   = out["team_name"].str.lower().str.replace(r"[^a-z0-9]+","_", regex=True) + "_" + SEASON.replace("-","")

    # Attach league/season
    out["league"] = LEAGUE
    out["season"] = SEASON

    # Minimum minutes filter is handled later when building vectors (keep all now)
    return out

def upsert_duckdb(player_df: pd.DataFrame):
    con = duckdb.connect("duckdb/fit.db")
    # Upsert teams
    teams = player_df[["team_id","team_name"]].drop_duplicates().copy()
    teams["league"] = LEAGUE
    teams["season"] = SEASON
    con.register("teams_df", teams)
    con.execute("""
        CREATE TABLE IF NOT EXISTS teams AS SELECT * FROM teams_df WHERE 0=1;
    """)
    con.execute("INSERT INTO teams SELECT * FROM teams_df;")
    con.unregister("teams_df")

    # Upsert players (basic info)
    players = player_df[["player_id","player_name","pos"]].drop_duplicates().copy()
    players.rename(columns={"pos":"primary_pos"}, inplace=True)
    players["dob"] = None
    players["nationality"] = None
    players["foot"] = None
    players["height_cm"] = None
    con.register("players_df", players)
    con.execute("""
        CREATE TABLE IF NOT EXISTS players AS SELECT * FROM players_df WHERE 0=1;
    """)
    con.execute("INSERT INTO players SELECT * FROM players_df;")
    con.unregister("players_df")

    # Upsert player season stats
    keep_cols = [
        "player_id","team_id","league","season","minutes",
        "progressive_passes_per90","progressive_carries_per90",
        "carries_into_final_third_per90","dribbles_completed_per90",
        "shots_per90","xg_per90","xa_per90",
        "tackles_interceptions_per90",
        "pressures_per90","aerials_won_pct",
        "crosses_into_box_per90","through_balls_completed_per90"
    ]
    con.register("ps_df", player_df[keep_cols].copy())
    con.execute("""
        CREATE TABLE IF NOT EXISTS player_season_stats AS SELECT * FROM ps_df WHERE 0=1;
    """)
    con.execute("INSERT INTO player_season_stats SELECT * FROM ps_df;")
    con.unregister("ps_df")

    # Build team season stats by minutes-weighted averages of players on the team
    # For features required by our team profile vector
    agg_expr = """
        team_id, ANY_VALUE(league) AS league, ANY_VALUE(season) AS season,
        SUM(minutes) AS minutes,
        SUM(progressive_passes_per90 * minutes)/NULLIF(SUM(minutes),0) AS progressive_passes_per90,
        SUM(progressive_carries_per90 * minutes)/NULLIF(SUM(minutes),0) AS progressive_carries_per90,
        0.0 AS crosses_into_box_per90,
        0.0 AS through_balls_completed_per90,
        -- proxy press intensity: pressures in att 3rd not available; use pressures_per90 average
        SUM(pressures_per90 * minutes)/NULLIF(SUM(minutes),0) AS pressures_att3rd_per90,
        SUM(aerials_won_pct * minutes)/NULLIF(SUM(minutes),0) AS aerials_win_pct,
        NULL AS possession_pct,
        NULL AS passes_per90,
        NULL AS opp_passes_allowed_per_def_action
    """
    team_stats = con.execute(f"""
        SELECT {agg_expr}
        FROM player_season_stats
        WHERE league = ? AND season = ?
        GROUP BY team_id
    """, [LEAGUE, SEASON]).fetch_df()

    con.register("ts_df", team_stats)
    con.execute("""
        CREATE TABLE IF NOT EXISTS team_season_stats AS SELECT * FROM ts_df WHERE 0=1;
    """)
    con.execute("INSERT INTO team_season_stats SELECT * FROM ts_df;")
    con.unregister("ts_df")

    con.close()

if __name__ == "__main__":
    frames = fetch_league_player_frames()
    players = build_player_master(frames)
    upsert_duckdb(players)
    print(f"✅ Ingested {players.shape[0]} player-season rows for {LEAGUE} {SEASON}")
