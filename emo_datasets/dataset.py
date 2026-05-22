from config import EXTRACTED_PATH
from preprocess import extract_bvp, extract_ecg, extract_eda, SIGNAL_FEATURES
import os
import pandas as pd
import numpy as np

ERROR_COLUMNS = ['dataset', 'subject_id', 'segment_id', 'window_start', 'signal', 'error_type', 'error_msg']

class Dataset:
    name: str
    path: str
    annotations_path: str
    sampling_rate: int
    fileformat: str = '.csv'
    splitchar: str = '-'
    data_offset: int = 0
    signals: list = ['ECG', 'EDA']

    def run(self, sample_size=None, thread_num=0, max_threads=1, window_time=6):
        filenames = sorted(f for f in os.listdir(self.path) if f.endswith(self.fileformat))
        subjects = [f.split(self.splitchar)[0] for f in filenames]

        if sample_size is not None:
            subjects = subjects[:sample_size]

        results = []
        errors = []

        subjects_for_thread = subjects[thread_num::max_threads]
        for s in subjects_for_thread:
            print(f'[{self.name} t{thread_num}/{max_threads}] Loading subject: {s}', flush=True)
            try:
                data, annotations = self.load_subject(s)
                processed, subject_error = self.process_subject(data, annotations, subject_id=s, window_time=window_time)
                results.append(processed)
                errors.append(subject_error)
            except Exception as e:
                print(f'[{self.name}] FAILED subject {s}: {type(e).__name__}: {e}', flush=True)
                continue

        results = pd.concat(results, ignore_index=True) if results else pd.DataFrame()
        errors = pd.concat(errors, ignore_index=True) if errors else pd.DataFrame(columns=ERROR_COLUMNS)
            
        return results, errors
    
    def load_subject(self, subject_id, cache=False):
        pass

    def extract_features(self, data, i, window_size):
        extractors = {'ECG': extract_ecg, 'BVP': extract_bvp, 'EDA': extract_eda}

        parts = []
        errors = []
        for sig in self.signals:
            try:
                parts.append(extractors[sig](data, i, window_size, self.sampling_rate))
            except Exception as e:
                nan_df = pd.DataFrame([{col: np.nan for col in SIGNAL_FEATURES[sig]}])
                parts.append(nan_df)
                errors.append((sig, e))

        return parts, errors
    
    def add_labels(self, combined, data, i, window_size, segment_id):
        combined['VALENCE'] = data[i:i+window_size]['VALENCE'].mean()
        combined['AROUSAL'] = data[i:i+window_size]['AROUSAL'].mean()
        return combined
    
    def get_segments(self, data):
        pass
    
    def post_process(self, features, annotations):
        return features

    def process_subject(self, data, annotations, subject_id, window_time):
        window_size = window_time * self.sampling_rate

        data = self.merge_with_annotations(data, annotations)
        output_filename = os.path.join(EXTRACTED_PATH, self.name, f'{subject_id}.csv')
        output_filename_errors = os.path.join(EXTRACTED_PATH, self.name, f'{subject_id}_errors.csv')

        results = []
        results_errors = []
        for segment_id, segment in self.get_segments(data):
            segment = segment.reset_index(drop=True) 
            for i in range(self.data_offset, len(segment) - window_size + 1, window_size):
                extracted, errors = self.extract_features(segment, i, window_size)

                for sig_name, e in errors:
                        print(f"[{self.name},{subject_id}]: skip window i={i} segment={segment_id}: {e}", flush=True)
                        window_data = {
                            'dataset':self.name,
                            'subject_id': subject_id,
                            'segment_id': segment_id,
                            'window_start': i,
                            'signal': sig_name,
                            'error_type': type(e).__name__,
                            'error_msg': str(e)
                        }
                        results_errors.append(window_data)
                    
                if extracted is not None:
                    combined = pd.concat(extracted, axis=1)
                    combined = self.add_labels(combined, segment, i, window_size, segment_id)
                    results.append(combined)

        final = pd.concat(results, ignore_index=True) if results else pd.DataFrame()
        final = self.post_process(final, annotations)
        final['SUBJECT_ID'] = subject_id
        results_errors = pd.DataFrame(results_errors)

        final.to_csv(output_filename, index=False)
        results_errors.to_csv(output_filename_errors, index=False)
        return final, results_errors
    
    def merge_with_annotations(self, sig, ann):
        return sig
    
    def merge_subjects_to_csv(self):
        dir = os.path.join(EXTRACTED_PATH, self.name)
        files = [
            os.path.join(dir, f) 
            for f in os.listdir(dir) 
            if not f.endswith('_errors.csv')
        ]
        dfs = [pd.read_csv(f) for f in files]
        
        result = pd.concat(dfs, ignore_index=True)
        final_filename = os.path.join(EXTRACTED_PATH, f'{self.name}.csv')
        result.to_csv(final_filename, index=False)