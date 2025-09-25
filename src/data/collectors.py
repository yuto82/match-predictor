import os
import pandas as pd
from typing import List, Dict
from pathlib import Path

class FootballDataCollector:
    def __init__(self, data_raw_path: str = "data/raw"):
        self.data_raw_path = Path(data_raw_path)

    def get_available_seasons(self) -> List[str]:
        if not self.data_raw_path.exists():
            raise FileNotFoundError(f"Folder {self.data_raw_path} not found")
    
        seasons = [
            file.name.replace("E0_", "").replace(".csv", "")
            for file in self.data_raw_path.glob("E0_*.csv")
        ]
        
        return sorted(seasons)

    def load_season(self, season: str) -> pd.DataFrame:
        file_path = self.data_raw_path / f"E0_{season}.csv"

        try:
            return pd.read_csv(file_path)
        except FileNotFoundError:
            raise FileNotFoundError(f"Season {season} not found")
        
    def load_all_seasons(self) -> pd.DataFrame:
        available_seasons = self.get_available_seasons()

        if not available_seasons:
            raise ValueError("No available seasons")
        
        all_seasons_data = []
        for season in available_seasons:
            try:
                df = self.load_season(season)
                df['season'] = season
                all_seasons_data.append(df)
            except Exception as e:
                print(f"Error loading season {season}: {e}")
                continue

        if not all_seasons_data:
            raise ValueError("No seasons were successfully loaded")

        return pd.concat(all_seasons_data, ignore_index=True)

    def load_selected_seasons(self, seasons: List[str]) -> pd.DataFrame:
        if not seasons:
            raise ValueError("No seasons specified")
        
        available_seasons = self.get_available_seasons()
        missing_seasons = [s for s in seasons if s not in available_seasons]

        if missing_seasons:
            raise ValueError(f"Seasons not found: {missing_seasons}")
        
        selected_seasons_data = []
        for season in seasons:
            try:
                df = self.load_season(season)
                df['season'] = season
                selected_seasons_data.append(df)
            except Exception as e:
                print(f"Error loading season {season}: {e}")
                continue
        
        if not selected_seasons_data:
            raise ValueError("No seasons were successfully loaded")
            
        return pd.concat(selected_seasons_data, ignore_index=True)