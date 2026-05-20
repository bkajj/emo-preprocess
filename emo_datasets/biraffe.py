from .dataset import Dataset
from config import BASE_DIR, raw_dataset_memory
import os
import pandas as pd

@raw_dataset_memory.cache
def _load_biraffe_subject(sub_id, path, annotations_path):
    biosigs = pd.read_csv(os.path.join(path, f'{sub_id}-BioSigs.csv'), sep=',')
    annotations = pd.read_csv(os.path.join(annotations_path, f'{sub_id}-Procedure.csv'), sep=';')
    annotations = annotations.rename(columns={'ANS-VALENCE':'VALENCE', 'ANS-AROUSAL':'AROUSAL'})
    return biosigs, annotations

class Biraffe(Dataset):
    name = 'BIRAFFE'
    path = os.path.join(BASE_DIR, 'BIRAFFE2', 'biosigs', 'BIRAFFE2-biosigs')
    annotations_path = os.path.join(BASE_DIR, 'BIRAFFE2', 'procedure', 'BIRAFFE2-procedure')
    sampling_rate = 1000

    def load_subject(self, sub_id):
        return _load_biraffe_subject(sub_id, self.path, self.annotations_path)
    
    def get_segments(self, data):
        return data.groupby('STIMULI_ID')
    
    def merge_with_annotations(self, sig, ann):
        parts = [1, 2]
        sigs = []
        stimuli_id = 0
        for p in parts:
            start_str = f'STIMULI PART {p} START'
            end_str = f'STIMULI PART {p} END'
            ann_start_idx = ann[ann['EVENT'] == start_str].index[0] + 1
            ann_end_idx = ann[ann['EVENT'] == end_str].index[0]
            ts_start = ann.loc[ann_start_idx]['TIMESTAMP']
            ts_end = ann.loc[ann_end_idx]['TIMESTAMP']
            
            ann_part = ann[(ann['TIMESTAMP'] >= ts_start) & (ann['TIMESTAMP'] < ts_end)]
            stimuli = ann_part[ann_part['EVENT'].isna()]
            for _, a in stimuli.iterrows():
                start = a['TIMESTAMP']
                end = a['TIMESTAMP'] + 6
                sig_fragment = sig[(sig['TIMESTAMP'] >= start) & (sig['TIMESTAMP'] < end)].copy()
                sig_fragment['STIMULI_ID'] = stimuli_id
                sig_fragment['VALENCE'] = a['VALENCE']
                sig_fragment['AROUSAL'] = a['AROUSAL']
                sigs.append(sig_fragment)
                stimuli_id += 1

        sig = pd.concat(sigs, ignore_index=True)
        return sig