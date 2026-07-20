import os
from joblib import Memory
from datetime import datetime
import yaml
import shutil

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EXTRACTED_PATH = os.path.join(BASE_DIR, 'extracted')
RESULTS_PATH = os.path.join(BASE_DIR, 'results')
CONFIG_PATH = os.environ.get('PIPELINE_CONFIG_PATH', 'configs/sample.yaml')

with open(CONFIG_PATH) as f:
    config = yaml.safe_load(f)

RUN_DIR_NAME = f"{datetime.now().strftime('%y-%m-%d_%H%M')}_{config['name']}" #date_time_namefromYAML
RUN_RESULT_PATH = os.path.join(RESULTS_PATH, RUN_DIR_NAME)

os.makedirs(os.path.join(RUN_RESULT_PATH, 'BIRAFFE'), exist_ok=True)
os.makedirs(os.path.join(RUN_RESULT_PATH, 'CASE'), exist_ok=True)
os.makedirs(os.path.join(RUN_RESULT_PATH, 'DEAP'), exist_ok=True)

shutil.copy(CONFIG_PATH, RUN_RESULT_PATH)


if config['cache']['features']:
    features_memory = Memory(location=os.path.join(BASE_DIR, '.cache/features/'), verbose=0)
else:
    features_memory = Memory(location=None, verbose=0)

if config['cache']['raw_signals']:
    raw_dataset_memory = Memory(location=os.path.join(BASE_DIR, '.cache/raw_signal_data/'), verbose=5)
else:
    raw_dataset_memory = Memory(location=None, verbose=5)