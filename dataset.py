from config import BASE_DIR, EXTRACTED_PATH
from preprocess import extract_bvp, extract_ecg, extract_eda
import os
import pandas as pd
import pickle

class Dataset:
    name: str
    path: str
    annotations_path: str
    sampling_rate: int
    fileformat: str = '.csv'
    splitchar: str = '-'
    data_offset: int = 0
    signals: list = ['ECG', 'EDA']

    def run(self, sample_size=None):
        filenames = sorted(f for f in os.listdir(self.path) if f.endswith(self.fileformat))
        subjects = [f.split(self.splitchar)[0] for f in filenames]

        if sample_size is not None:
            subjects = subjects[:sample_size]

        results = []
        for s in subjects:
            print(f'Loading {self.name} subject: {s}', flush=True)
            data, annotations = self.load_subject(s)
            results.append(self.process_subject(data, annotations, subject_id=s, window_time=5))
        results = pd.concat(results, ignore_index=True)

        if sample_size is None:
            self.merge_subjects_to_csv()
            
        return results
    
    def load_subject(self, subject_id):
        pass

    def extract_features(self, data, i, window_size):
        extractors = {'ECG': extract_ecg, 'BVP': extract_bvp, 'EDA': extract_eda}
        parts = [extractors[sig](data, i, window_size, self.sampling_rate) for sig in self.signals]
        return parts
    
    def add_labels(self, combined, data, i, window_size, segment_id):
        combined['VALENCE'] = data[i:i+window_size]['VALENCE'].mean()
        combined['AROUSAL'] = data[i:i+window_size]['AROUSAL'].mean()
        return combined
    
    def get_segments(self, data):
        return [(None, data)] # workaround for now
    
    def post_process(self, features, annotations):
        return features

    def process_subject(self, data, annotations, subject_id, window_time):
        window_size = window_time * self.sampling_rate

        data = self.merge_with_annotations(data, annotations)
        output_filename = os.path.join(EXTRACTED_PATH, self.name, f'{subject_id}.csv')

        results = []
        for segment_id, segment in self.get_segments(data):
            segment = segment.reset_index(drop=True) 
            for i in range(self.data_offset, len(segment) - window_size, window_size):
                try:
                    extracted = self.extract_features(segment, i, window_size)
                    combined = pd.concat(extracted, axis=1)
                    combined = self.add_labels(combined, segment, i, window_size, segment_id)
                    results.append(combined)
                except Exception as e:
                    print(f"Skip window i={i} segment={segment_id}: {e}", flush=True)
                    continue
        
        final = pd.concat(results, ignore_index=True)
        final = self.post_process(final, annotations)
        final.to_csv(output_filename, index=False)
        return final
    
    def merge_with_annotations(self, sig, ann):
        return sig
    
    def merge_subjects_to_csv(self):
        dir = os.path.join(EXTRACTED_PATH, self.name)
        files = [os.path.join(dir, f) for f in os.listdir(dir)]
        dfs = [pd.read_csv(f) for f in files]
        
        result = pd.concat(dfs, ignore_index=True)
        final_filename = os.path.join(EXTRACTED_PATH, f'{self.name}.csv')
        result.to_csv(final_filename)
    



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
        with open(os.path.join(self.path, f'{sub_id}.dat'), 'rb') as f:
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
    
    def add_labels(self, combined, data, i, window_size, segment_id):
        combined['video_id'] = segment_id
        return combined
    
    def get_segments(self, data):
        return data.groupby('video_id')
    
    def post_process(self, features, annotations):
        return pd.merge(features, annotations, on='video_id')
    
    def merge_with_annotations(self, sig, ann):
        return sig