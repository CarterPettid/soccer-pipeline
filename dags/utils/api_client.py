import requests
import os

class FootballDataClient:
    BASE_URL = "https://api.football-data.org/v4"
    
    def __init__(self):
        self.api_key = os.getenv("FOOTBALL_DATA_API_KEY")
        if not self.api_key:
            raise ValueError("FOOTBALL_DATA_API_KEY not set")
        self.headers = {"X-Auth-Token": self.api_key}
    
    def get_matches(self, competition="PL", season=2024):
        """Fetch matches for a competition and season."""
        url = f"{self.BASE_URL}/competitions/{competition}/matches"
        params = {"season": season}
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()["matches"]
    
    def get_standings(self, competition="PL", season=2024):
        """Fetch league standings."""
        url = f"{self.BASE_URL}/competitions/{competition}/standings"
        params = {"season": season}
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()["standings"]

    def get_scorers(self, competition="PL", season=2024, limit=20):
        """Fetch top scorers for a competition."""
        url = f"{self.BASE_URL}/competitions/{competition}/scorers"
        params = {"season": season, "limit": limit}
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()["scorers"]
    
    def get_teams(self, competition="PL", season=2024):
        """Fetch all teams in a competition."""
        url = f"{self.BASE_URL}/competitions/{competition}/teams"
        params = {"season": season}
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()["teams"]