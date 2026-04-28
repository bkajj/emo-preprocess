import os
from joblib import Memory 

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EXTRACTED_PATH = os.path.join(BASE_DIR, 'extracted')

# Features are always cached
features_memory = Memory(location=os.path.join(BASE_DIR, '.cache/features/'), verbose=0)

if os.environ.get('USE_DATASET_MEMORY') == '1':
    raw_dataset_memory = Memory(location=os.path.join(BASE_DIR, '.cache/raw_signal_data/'), verbose=5)
else:
    raw_dataset_memory = Memory(location=None, verbose=5)