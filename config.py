import os
from joblib import Memory
import yaml

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EXTRACTED_PATH = os.path.join(BASE_DIR, 'extracted')
CONFIG_PATH = os.environ.get('PIPELINE_CONFIG_PATH', 'configs/sample.yaml')

with open(CONFIG_PATH) as f:
    config = yaml.safe_load(f)

if config['cache']['features']:
    features_memory = Memory(location=os.path.join(BASE_DIR, '.cache/features/'), verbose=0)
else:
    features_memory = Memory(location=None, verbose=0)

if config['cache']['raw_signals']:
    raw_dataset_memory = Memory(location=os.path.join(BASE_DIR, '.cache/raw_signal_data/'), verbose=5)
else:
    raw_dataset_memory = Memory(location=None, verbose=5)