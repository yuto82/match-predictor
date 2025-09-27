import os
import pandas as pd
from typing import List, Dict

class DataProcessor:
    def validate_data(self, df: pd.DataFrame) -> Dict:
        validation_result = {
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'stats': {}
        }

        self._run_all_validations(df, validation_result)
        self._collect_stats(df, validation_result)

        return validation_result

    def _run_all_validations(self, df: pd.DataFrame, result: Dict):
        validators = [
            self._validate_columns,
            self._validate_numeric_columns,
            self._validate_date_column, 
            self._validate_score_values,
            self._validate_result_values,
            self._validate_missing_values,
            self._validate_duplicate_values
        ]

        for validator in validators:
            validator(df, result)


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
                pd.to_datetime(df['Date'], errors='coerce', dayfirst=True)
        except Exception as e:
            result['errors'].append(f"Invalid date format: {str(e)}")
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
        def date_range():
            if len(df) == 0 or 'Date' not in df.columns:
                return None
            try:
                dates = pd.to_datetime(df['Date'], errors='coerce', dayfirst=True).dropna()
                return (dates.min(), dates.max()) if len(dates) > 0 else None
            except:
                return None

        def unique_teams():
            return len(set(df['HomeTeam'].unique()) | set(df['AwayTeam'].unique()))

        result['stats'] = {
            'total_rows': len(df),
            'unique_teams': unique_teams(),
            'date_range': date_range(),
            'null_count': df.isnull().sum().sum()
        }

    def handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        critical_columns = ['Date', 'HomeTeam', 'AwayTeam', 'FTR']

        original_rows = len(df)
        cleaned_df = df.copy()

        cleaned_df['FTHG'] = cleaned_df['FTHG'].fillna(0)
        cleaned_df['FTAG'] = cleaned_df['FTAG'].fillna(0)

        cleaned_df = cleaned_df.dropna(subset=critical_columns)

        removed_rows = original_rows - len(cleaned_df)
        if removed_rows > 0:
            print(f"Removed {removed_rows} rows with missing critical data")

        return cleaned_df

    def clean_date_values(self, df: pd.DataFrame) -> pd.DataFrame:
        cleaned_df = df.copy()

        cleaned_df['Date'] = pd.to_datetime(cleaned_df['Date'], errors='coerce', dayfirst=True)
        cleaned_df = cleaned_df.dropna(subset=['Date'])

        return cleaned_df

    def normalize_team_names(self, df: pd.DataFrame) -> pd.DataFrame:
        team_mapping = {
            'Man United': 'Manchester United',
            'Man City': 'Manchester City',
            'Leicester': 'Leicester City',
            'Tottenham': 'Tottenham Hotspur',
            'Newcastle': 'Newcastle United',
            'West Ham': 'West Ham United',
            'Brighton': 'Brighton & Hove Albion',
            'Wolves': 'Wolverhampton Wanderers',
            'Sheffield Utd': 'Sheffield United',
            'Nottm Forest': 'Nottingham Forest',
            "Nott'm Forest": 'Nottingham Forest',
            'Bournemouth': 'AFC Bournemouth',
            'Luton': 'Luton Town',
            'Burnley': 'Burnley FC',
            'Brentford': 'Brentford FC',
            'Fulham': 'Fulham FC'
        }
        
        normalized_df = df.copy()
        
        normalized_df['HomeTeam'] = normalized_df['HomeTeam'].replace(team_mapping)
        normalized_df['AwayTeam'] = normalized_df['AwayTeam'].replace(team_mapping)
        
        return normalized_df

    def create_basic_features(self, df: pd.DataFrame) -> pd.DataFrame:
        features_df = df.copy()

        features_df['total_goals'] = features_df['FTHG'] + features_df['FTAG']
        features_df['goal_difference'] = features_df['FTHG'] - features_df['FTAG']

        features_df['day'] = features_df['Date'].dt.dayofweek
        features_df['year'] = features_df['Date'].dt.year
        features_df['month'] = features_df['Date'].dt.month
        features_df['is_weekend'] = features_df['day'].isin([5, 6])

        if 'FTR' in features_df.columns:
            features_df['result'] = features_df['FTR'].map({'H': 2, 'D': 1, 'A': 0})

        features_df['is_home'] = 1
        features_df['is_away'] = 0

        features_df['is_high_scoring'] = (features_df['total_goals'] >= 3).astype(int)
        features_df['is_low_scoring'] = (features_df['total_goals'] <= 1).astype(int)
        features_df['is_close_match'] = (features_df['goal_difference'].abs() <= 1).astype(int)

        return features_df

    def process_data(self, df: pd.DataFrame) -> pd.DataFrame:
        validation = self.validate_data(df)
        if not validation['is_valid']:
            raise ValueError(f"Data validation failed: {validation['errors']}")

        processing_steps = [
            self.handle_missing_values,
            self.clean_date_values,
            self.normalize_team_names,
            self.create_basic_features
        ]
        
        processed_df = df.copy()
        for i, step in enumerate(processing_steps):
            try:
                processed_df = step(processed_df)
                print(f"Step {i+1}/{len(processing_steps)} completed: {step.__name__}")
            except Exception as e:
                raise ValueError(f"Error in step {step.__name__}: {str(e)}")

        final_validation = self.validate_data(processed_df)
        if final_validation['warnings']:
            print(f"Processing warnings: {final_validation['warnings']}")
        
        print(f"Data processing complete: {len(df)} -> {len(processed_df)} rows")
        
        return processed_df