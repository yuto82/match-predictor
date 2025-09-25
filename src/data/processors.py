import os
import pandas as pd
from typing import List, Dict

class DataProcessor:
    def validate_data(self, df: pd.DataFrame) -> bool:
        required_cols = ['Date', 'HomeTeam', 'AwayTeam', 'FTHG', 'FTAG', 'FTR']
        
        return all(col in df.columns for col in required_cols)
    
    def clean_historical_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.drop_duplicates()
        df['Date'] = pd.to_datetime(df['Date'], format='%d/%m/%Y')
        df['result'] = df['FTR'].map({'H': 2, 'D': 1, 'A': 0})
        
        return df

    def handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        pass

    def create_basic_features(self, df: pd.DataFrame) -> pd.DataFrame:
        pass


    def normalize_team_names(self, df: pd.DataFrame) -> pd.DataFrame:
        team_mapping = {
            'Man United': 'Manchester United',
            'Man City': 'Manchester City',
            'Leicester': 'Leicester City',
            'Tottenham': 'Tottenham Hotspur',
            'Newcastle': 'Newcastle United',
            'West Ham': 'West Ham United',
            'Brighton': 'Brighton & Hove Albion',
            'Crystal Palace': 'Crystal Palace',
            'Aston Villa': 'Aston Villa',
            'Wolves': 'Wolverhampton Wanderers',
            'Sheffield United': 'Sheffield Utd',
            'Sheffield Utd': 'Sheffield United',
            'Norwich': 'Norwich City',
            'Watford': 'Watford FC',
            'Burnley': 'Burnley FC',
            'Leeds': 'Leeds United',
            'Brentford': 'Brentford FC',
            'Fulham': 'Fulham FC',
            'Bournemouth': 'AFC Bournemouth',
            'Luton': 'Luton Town',
            'Nottm Forest': 'Nottingham Forest',
            "Nott'm Forest": 'Nottingham Forest',
            'QPR': 'Queens Park Rangers',
            'Swansea': 'Swansea City',
            'Cardiff': 'Cardiff City',
            'Hull': 'Hull City',
            'Stoke': 'Stoke City',
            'Middlesbrough': 'Middlesbrough FC',
            'Wigan': 'Wigan Athletic',
            'Blackburn': 'Blackburn Rovers',
            'Bolton': 'Bolton Wanderers',
            'Birmingham': 'Birmingham City',
            'Blackpool': 'Blackpool FC',
            'Reading': 'Reading FC',
            'Huddersfield': 'Huddersfield Town'
        }
        df['HomeTeam'] = df['HomeTeam'].replace(team_mapping)
        df['AwayTeam'] = df['AwayTeam'].replace(team_mapping)
        return df
