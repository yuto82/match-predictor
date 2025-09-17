import os
from dataclasses import dataclass

@dataclass
class Config:
    DATA_RAW_PATH = "data/raw"
    DATA_PROCESSED_PATH = "data/processed"
    MODELS_PATH = "models"
    
    RANDOM_STATE = 42
    TEST_SIZE = 0.2
    FORM_MATCHES = 5
    
    FEATURE_COLUMNS = [
        'HomeTeam_encoded', 'AwayTeam_encoded',
        'Home_Form', 'Away_Form',
        'Total_Goals', 'Goal_Difference'
    ]