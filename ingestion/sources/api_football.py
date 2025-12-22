import requests
import time
from typing import Dict, List, Optional
import os
import sys

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from ingestion.config import API_BASE_URL, API_KEY

class APIFootballClient:
    def __init__(self):
        self.base_url = API_BASE_URL
        self.headers = {
            "X-RapidAPI-Key": API_KEY,
            "X-RapidAPI-Host": "v3.football.api-sports.io"
        }
    
    def _make_request(self, endpoint: str, params: Dict) -> Dict:
        url = f"{self.base_url}/{endpoint}"
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()
    
    def get_teams(self, league: int, season: int) -> List[Dict]:
        params = {"league": league, "season": season}
        data = self._make_request("teams", params)
        
        if data.get("errors"):
            raise Exception(f"API errors: {data['errors']}")
        
        return data.get("response", [])
    
    def get_fixtures(self, league: int, season: int, date_from: Optional[str] = None, date_to: Optional[str] = None, date: Optional[str] = None) -> List[Dict]:
        all_data = []
        page = 1
        
        while True:
            params = {"league": league, "season": season}
            
            if date:
                params["date"] = date
            elif date_from and date_to:
                params["from"] = date_from
                params["to"] = date_to
            
            data = self._make_request("fixtures", params)
            
            if data.get("errors"):
                errors = data["errors"]
                if "page" in str(errors).lower():
                    break
                raise Exception(f"API errors: {errors}")
            
            all_data.extend(data.get("response", []))
            
            paging = data.get("paging", {})
            if paging.get("total", 1) <= 1 or page >= paging.get("total", 1):
                break
            
            page += 1
            params["page"] = page
            time.sleep(0.1)
        
        return all_data
    
    def get_standings(self, league: int, season: int) -> List[Dict]:
        params = {"league": league, "season": season}
        data = self._make_request("standings", params)
        
        if data.get("errors"):
            raise Exception(f"API errors: {data['errors']}")
        
        response = data.get("response", [])
        if response:
            standings = response[0].get("league", {}).get("standings", [])
            if standings and len(standings) > 0:
                return standings[0]
        
        return []
    
    def get_top_scorers(self, league: int, season: int) -> List[Dict]:
        params = {"league": league, "season": season}
        data = self._make_request("players/topscorers", params)
        
        if data.get("errors"):
            raise Exception(f"API errors: {data['errors']}")
        
        all_data = []
        response = data.get("response", [])
        for player in response:
            player_data = player.get("player", {})
            stats = player.get("statistics", [])
            if stats:
                stat = stats[0]
                record = {**player_data, **stat}
                all_data.append(record)
        
        return all_data

