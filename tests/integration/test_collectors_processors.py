import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.data.collectors import FootballDataCollector
from src.data.processors import DataProcessor
import pandas as pd

def test_collectors():
    print("TESTING COLLECTORS")
    
    try:
        collector = FootballDataCollector()
        
        seasons = collector.get_available_seasons()
        print(f"Available seasons: {seasons}")
        
        print(f"\nLoading season: {seasons[0]}")
        df = collector.load_season(seasons[0])
        print(f"Loaded {len(df)} rows")
        print(f"Columns: {list(df.columns)}")
        print(f"Date range: {df['Date'].min()} - {df['Date'].max()}")
        
        print(f"\nLoading all seasons...")
        all_df = collector.load_all_seasons()
        print(f"Loaded {len(all_df)} rows from {len(seasons)} seasons")
        print(f"Unique teams: {len(set(all_df['HomeTeam'].unique()) | set(all_df['AwayTeam'].unique()))}")
        
        return all_df
        
    except Exception as e:
        print(f"ERROR in collectors: {e}")
        return None

def test_processors(df):
    print("TESTING PROCESSORS")
    
    if df is None:
        print("ERROR: No data available for processing")
        return None
    
    try:
        processor = DataProcessor()
        
        print("Validating data...")
        validation = processor.validate_data(df)
        print(f"Validation: {'PASS' if validation['is_valid'] else 'FAIL'}")
        if validation['errors']:
            print(f"Errors: {validation['errors']}")
        if validation['warnings']:
            print(f"Warnings: {validation['warnings']}")
        print(f"Statistics: {validation['stats']}")
        
        if not validation['is_valid']:
            print("ERROR: Validation failed, skipping processing")
            return None
        
        print(f"\nProcessing data...")
        processed_df = processor.process_data(df)
        print(f"Processed: {len(df)} -> {len(processed_df)} rows")
        
        print(f"\nProcessing results:")
        print(f"Columns: {list(processed_df.columns)}")
        print(f"Date range: {processed_df['Date'].min()} - {processed_df['Date'].max()}")
        print(f"Result distribution: {processed_df['result'].value_counts().to_dict()}")
        print(f"Unique teams: {len(set(processed_df['HomeTeam'].unique()) | set(processed_df['AwayTeam'].unique()))}")
        
        print(f"\nSample processed data:")
        print(processed_df[['Date', 'HomeTeam', 'AwayTeam', 'FTHG', 'FTAG', 'result', 'total_goals', 'goal_difference']].head())
        
        return processed_df
        
    except Exception as e:
        print(f"ERROR in processors: {e}")
        import traceback
        traceback.print_exc()
        return None

def test_data_quality(df):
    print("DATA QUALITY ANALYSIS")
    
    if df is None:
        print("ERROR: No data available for analysis")
        return
    
    try:
        print("Missing values analysis:")
        null_counts = df.isnull().sum()
        if null_counts.any():
            print(f"Missing values found: {null_counts[null_counts > 0].to_dict()}")
        else:
            print("No missing values found")
        
        print(f"\nDuplicate analysis:")
        duplicates = df.duplicated().sum()
        if duplicates > 0:
            print(f"Found {duplicates} duplicates")
        else:
            print("No duplicates found")
        
        print(f"\nResult distribution:")
        result_counts = df['result'].value_counts()
        print(f"Home wins: {result_counts.get(2, 0)} ({result_counts.get(2, 0)/len(df)*100:.1f}%)")
        print(f"Draws: {result_counts.get(1, 0)} ({result_counts.get(1, 0)/len(df)*100:.1f}%)")
        print(f"Away wins: {result_counts.get(0, 0)} ({result_counts.get(0, 0)/len(df)*100:.1f}%)")
        
        print(f"\nGoal statistics:")
        print(f"Average home goals: {df['FTHG'].mean():.2f}")
        print(f"Average away goals: {df['FTAG'].mean():.2f}")
        print(f"Average total goals: {df['total_goals'].mean():.2f}")
        
        print(f"\nTemporal analysis:")
        print(f"Seasons: {sorted(df['season'].unique())}")
        print(f"Years: {sorted(df['year'].unique())}")
        print(f"Matches per year: {df.groupby('year').size().to_dict()}")
        
    except Exception as e:
        print(f"ERROR in quality analysis: {e}")

def run_performance_tests(df):
    print("PERFORMANCE ANALYSIS")
    
    if df is None:
        print("ERROR: No data available for performance testing")
        return
    
    import time
    
    try:
        processor = DataProcessor()
        
        steps = [
            ("Validation", processor.validate_data),
            ("Missing Values", processor.handle_missing_values),
            ("Date Cleaning", processor.clean_date_values),
            ("Team Normalization", processor.normalize_team_names),
            ("Feature Creation", processor.create_basic_features)
        ]
        
        print("Step performance:")
        test_df = df.copy()
        
        for step_name, step_func in steps:
            start_time = time.time()
            
            if step_name == "Validation":
                result = step_func(test_df)
            else:
                test_df = step_func(test_df)
            
            execution_time = time.time() - start_time
            print(f"{step_name}: {execution_time:.4f} seconds")
    
        start_time = time.time()
        processed_df = processor.process_data(df)
        total_time = time.time() - start_time
        
        print(f"\nFull pipeline: {total_time:.4f} seconds")
        print(f"Processing rate: {len(df)/total_time:.0f} rows/second")
        
    except Exception as e:
        print(f"ERROR in performance testing: {e}")

def main():
    print("FOOTBALL DATA PIPELINE TEST SUITE")
    
    df = test_collectors()
    
    processed_df = test_processors(df)

    test_data_quality(processed_df)
    
    if df is not None:
        run_performance_tests(df)
    
    if processed_df is not None:
        print("ALL TESTS COMPLETED SUCCESSFULLY")
        print(f"Final statistics:")
        print(f"   Processed matches: {len(processed_df)}")
        print(f"   Unique teams: {len(set(processed_df['HomeTeam'].unique()) | set(processed_df['AwayTeam'].unique()))}")
        print(f"   Seasons: {len(processed_df['season'].unique())}")
        print(f"   Features: {len(processed_df.columns)}")
    else:
        print("TESTS FAILED")

if __name__ == "__main__":
    main()