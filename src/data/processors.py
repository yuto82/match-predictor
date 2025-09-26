import os
import pandas as pd
from typing import List, Dict

class DataProcessor:
    def validate_data(self, df: pd.DataFrame) -> bool:
        validation_result = {
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'stats': {}
        }

        self._validate_columns(df, validation_result)
        if not validation_result['is_valid']:
            return validation_result
        
        self._validate_numeric_columns(df, validation_result)
        self._validate_date_column(df, validation_result)
        self._validate_score_values(df, validation_result)
        self._validate_result_values(df, validation_result)
        
        self._validate_missing_values(df, validation_result)
        self._validate_duplicate_values(df, validation_result)
        
        self._collect_stats(df, validation_result)

        return validation_result
    
    def _validate_columns(self, df: pd.DataFrame, result: Dict):
        required_columns = ['Date', 'HomeTeam', 'AwayTeam', 'FTHG', 'FTAG', 'FTR']

        missing_columns = [column for column in required_columns if column not in df.columns]
        if missing_columns:
            result['errors'].append(f"Missing columns: {missing_columns}")
            result['is_valid'] = False

    def _validate_numeric_columns(self, df: pd.DataFrame, result: Dict):
        numeric_columns = ['FTHG', 'FTAG']

        for column in numeric_columns:
            if column in df.columns and not pd.api.types.is_numeric_dtype(df[column]):
                result['errors'].append(f"{column} is not numeric")
                result['is_valid'] = False

    def _validate_date_column(self, df: pd.DataFrame, result: Dict):
        try:
            if 'Date' in df.columns:
                pd.to_datetime(df['Date'])
        except:
            result['errors'].append("Invalid date format")
            result['is_valid'] = False

    def _validate_score_values(self, df: pd.DataFrame, result: Dict):
        for column in ['FTHG', 'FTAG']:
            if (df[column] < 0).any():
                result['errors'].append(f"Negative values in {column}")
                result['is_valid'] = False
    
    def _validate_result_values(self, df: pd.DataFrame, result: Dict):
        valid_results = {'H', 'D', 'A'}
        invalid_results = set(df['FTR'].dropna().unique()) - valid_results

        if invalid_results:
            result['errors'].append(f"Invalid results: {invalid_results}")
            result['is_valid'] = False

    def _validate_missing_values(self, df: pd.DataFrame, result: Dict):
        target_columns = ['Date', 'HomeTeam', 'AwayTeam', 'FTHG', 'FTAG', 'FTR']

        null_counts = df[target_columns].isnull().sum()
        if null_counts.any():
            result['warnings'].append(f"Null values: {null_counts[null_counts > 0].to_dict()}")
        
    def _validate_duplicate_values(self, df: pd.DataFrame, result: Dict):
        key_columns = ['Date', 'HomeTeam', 'AwayTeam']
        
        duplicates = df.duplicated(key_columns).sum()
        if duplicates > 0:
            result['warnings'].append(f"Found {duplicates} duplicate matches")

    def _collect_stats(self, df: pd.DataFrame, result: Dict):
        result['stats'] = {
            'total_rows': len(df),
            'unique_teams': len(set(df['HomeTeam'].unique()) | set(df['AwayTeam'].unique())),
            'date_range': (df['Date'].min(), df['Date'].max()) if len(df) > 0 else None,
            'null_count': df.isnull().sum().sum()
        }
