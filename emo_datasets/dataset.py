from config import EXTRACTED_PATH
from preprocess import extract_bvp, extract_ecg, extract_eda
import os
import pandas as pd

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