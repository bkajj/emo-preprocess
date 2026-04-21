import os

dataset_names = ['biraffe', 'case', 'deap']

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

BIRAFFE_PATH = os.path.join(BASE_DIR, 'BIRAFFE2', 'biosigs', 'BIRAFFE2-biosigs')# 1 kHz
BIRAFFE_ANNOTATIONS_PATH = os.path.join(BASE_DIR, 'BIRAFFE2', 'procedure', 'BIRAFFE2-procedure')

CASE_PATH = os.path.join(BASE_DIR, 'CASE', 'data', 'interpolated', 'physiological')# 1 kHz
CASE_ANNOTATIONS_PATH = os.path.join(BASE_DIR, 'CASE', 'data', 'interpolated', 'annotations')

DEAP_PATH = os.path.join(BASE_DIR, 'DEAP', 'data_preprocessed_python', ) # 128 Hz    

EXTRACTED_PATH = os.path.join(BASE_DIR, 'extracted')