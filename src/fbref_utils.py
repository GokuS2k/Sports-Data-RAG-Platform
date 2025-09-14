"""Helpers for interacting with the FBref website."""

import requests
from bs4 import BeautifulSoup


def fetch_page(url: str) -> BeautifulSoup:
    """Fetch a web page and parse it with BeautifulSoup."""
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return BeautifulSoup(response.text, "html.parser")
