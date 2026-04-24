from .dataset import Dataset
from config import BASE_DIR
import os
import pandas as pd

class Biraffe(Dataset):
    name = 'BIRAFFE'
    path = os.path.join(BASE_DIR, 'BIRAFFE2', 'biosigs', 'BIRAFFE2-biosigs')
    annotations_path = os.path.join(BASE_DIR, 'BIRAFFE2', 'procedure', 'BIRAFFE2-procedure')
    sampling_rate = 1000

    def load_subject(self, sub_id):
        biosigs = pd.read_csv(os.path.join(self.path, f'{sub_id}-BioSigs.csv'), sep=',')
        annotations = pd.read_csv(os.path.join(self.annotations_path, f'{sub_id}-Procedure.csv'), sep=';')
        annotations = annotations.rename(columns={'ANS-VALENCE':'VALENCE', 'ANS-AROUSAL':'AROUSAL'})
        return biosigs, annotations
    
    def merge_with_annotations(self, sig, ann):
        parts = [1, 2]
        sigs = []
        anns = []
        for p in parts:
            start_str = f'STIMULI PART {p} START'
            end_str = f'STIMULI PART {p} END'
            ann_start_idx = ann[ann['EVENT'] == start_str].index[0] + 1
            ann_end_idx = ann[ann['EVENT'] == end_str].index[0]
            ts_start = ann.loc[ann_start_idx]['TIMESTAMP']
            ts_end = ann.loc[ann_end_idx]['TIMESTAMP']
            
            ann_part = ann[(ann['TIMESTAMP'] >= ts_start) & (ann['TIMESTAMP'] < ts_end)]
            sig_part = sig[(sig['TIMESTAMP'] >= ts_start) & (sig['TIMESTAMP'] < ts_end)]
            anns.append(ann_part)
            sigs.append(sig_part)
            
        sig = pd.concat(sigs)
        ann = pd.concat(anns)
        ann = ann[['TIMESTAMP', 'VALENCE', 'AROUSAL']]
        
        result = pd.merge_asof(sig, ann, on='TIMESTAMP')
        return result