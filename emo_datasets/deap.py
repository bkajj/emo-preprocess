from .dataset import Dataset
from config import BASE_DIR, raw_dataset_memory
import os
import pandas as pd
import pickle

@raw_dataset_memory.cache
def _load_deap_subject(sub_id, path):
    with open(os.path.join(path, f'{sub_id}.dat'), 'rb') as f:
        deap = pickle.load(f, encoding='latin1')

    annotations = pd.DataFrame(deap['labels'][:, :2], columns=['valence', 'arousal'])
    annotations = annotations.rename(columns={'valence': 'VALENCE', 'arousal': 'AROUSAL'})
    annotations['video_id'] = range(40)
    
    dfs = []
    deap_data = deap['data'][:, [36, 38], :]
    for i, video in enumerate(deap_data):
        df = pd.DataFrame(video.T, columns=['EDA', 'BVP'])
        df['video_id'] = i
        dfs.append(df)

    deap = pd.concat(dfs, ignore_index=True)
    return deap, annotations

class Deap(Dataset):
    name = 'DEAP'
    path = os.path.join(BASE_DIR, 'DEAP', 'data_preprocessed_python')
    annotations_path = os.path.join(BASE_DIR, 'DEAP', 'data_preprocessed_python')
    sampling_rate = 128
    fileformat = '.dat'
    splitchar = '.'
    data_offset = 384
    signals = ['BVP', 'EDA']
                
    def load_subject(self, sub_id):
        return _load_deap_subject(sub_id, self.path)
    
    def add_labels(self, combined, data, i, window_size, segment_id):
        combined['video_id'] = segment_id
        return combined
    
    def get_segments(self, data):
        return data.groupby('video_id')
    
    def post_process(self, features, annotations):
        return pd.merge(features, annotations, on='video_id')
    
    def merge_with_annotations(self, sig, ann):
        return sig
    
