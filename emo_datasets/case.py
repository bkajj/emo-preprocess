from .dataset import Dataset
from config import BASE_DIR
import os
import pandas as pd

class Case(Dataset):
    name = 'CASE'
    path = os.path.join(BASE_DIR, 'CASE', 'data', 'interpolated', 'physiological')
    annotations_path = os.path.join(BASE_DIR, 'CASE', 'data', 'interpolated', 'annotations')
    sampling_rate = 1000
    splitchar = '.'
    
    def load_subject(self, sub_id):
        biosigs = pd.read_csv(os.path.join(self.path, f'{sub_id}.csv'), sep=',')
        biosigs = biosigs[['daqtime', 'ecg', 'gsr', 'video']].rename(columns={'gsr': 'EDA', 'ecg': 'ECG', 'daqtime':'TIMESTAMP', 'video':'VIDEO_ID'})
        
        annotations = pd.read_csv(os.path.join(self.annotations_path, f'{sub_id}.csv'), sep=',')
        annotations = annotations.rename(columns={'jstime': 'TIMESTAMP', 'valence': 'VALENCE', 'arousal': 'AROUSAL', 'video':'VIDEO_ID'})
        return biosigs, annotations
    
    def merge_with_annotations(self, sig, ann):
        sig = sig[sig['VIDEO_ID'].isin(range(1, 9))].copy().sort_values('TIMESTAMP')
        ann = ann[ann['VIDEO_ID'].isin(range(1, 9))].copy().sort_values('TIMESTAMP')
        result = pd.merge_asof(sig, ann[['TIMESTAMP', 'VALENCE', 'AROUSAL']], on='TIMESTAMP')
        return result
    