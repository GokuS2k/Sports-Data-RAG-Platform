"""Example script to ingest player data from FBref."""

from ..fbref_utils import fetch_page
from ..db import get_connection


PLAYERS_URL = "https://fbref.com/en/players/"


def ingest_players() -> None:
    """Fetch a page from FBref and store its title in the database."""
    soup = fetch_page(PLAYERS_URL)
    conn = get_connection()
    conn.execute("CREATE TABLE IF NOT EXISTS players (page_title TEXT)")
    conn.execute("INSERT INTO players VALUES (?)", [soup.title.string if soup.title else "Unknown"])
    conn.close()


if __name__ == "__main__":
    ingest_players()
