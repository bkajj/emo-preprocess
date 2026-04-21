import pandas as pd
import numpy as np
import neurokit2 as nk
import warnings;
warnings.filterwarnings('ignore')
from config import *

# 1000Hz wiec 1000x na sekunde jest wykonywany pomiar, wiec pierwsze okno zaczyna sie od 0, nastepne od 5000

def add_annotations_biraffe(sig, ann):
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

def add_annotations_case(sig, ann):
    sig = sig[sig['VIDEO_ID'].isin(range(1, 9))].copy().sort_values('TIMESTAMP')
    ann = ann[ann['VIDEO_ID'].isin(range(1, 9))].copy().sort_values('TIMESTAMP')
    result = pd.merge_asof(sig, ann[['TIMESTAMP', 'VALENCE', 'AROUSAL']], on='TIMESTAMP')
    return result
        

def preprocess_signals(data, annotations, dataset_name, subject_id, window_time = 5, sampling_rate = 1000):
    window_size = window_time * sampling_rate # number of samples
    
    if dataset_name == 'biraffe':
        data = add_annotations_biraffe(data, annotations)
        output_filename = os.path.join(EXTRACTED_PATH, 'biraffe', f'{subject_id}.csv')
    elif dataset_name == 'case':
        data = add_annotations_case(data, annotations)
        output_filename = os.path.join(EXTRACTED_PATH, 'case', f'{subject_id}.csv')
        
    results = []
    for i in range(0, len(data) - window_size, window_size):
        try:
            ecg = extract_ecg(data, i, window_size, sampling_rate)
            eda = extract_eda(data, i, window_size, sampling_rate)
            
            combined = pd.concat([ecg, eda], axis=1)
            combined['VALENCE'] = data[i:i+window_size]['VALENCE'].mean()
            combined['AROUSAL'] = data[i:i+window_size]['AROUSAL'].mean()
            results.append(combined)
        except:
            continue
            
    final = pd.concat(results, ignore_index=True)
    final.to_csv(output_filename, index=False)
    return final

def preprocess_deap(data, annotations, subject_id, window_time = 5, sampling_rate = 128):
    window_size = window_time * sampling_rate # number of samples
    results = []
    
    output_filename = os.path.join(EXTRACTED_PATH, 'deap', f'{subject_id}.csv')
    
    for video_id, group in data.groupby('video_id'):
        group = group.reset_index(drop=True)
        for i in range(384, len(group) - window_size, window_size): #384 cuz first 3 seconds (384 samples) are baseline
            try:
                bvp = extract_bvp(group, i, window_size, sampling_rate)
                eda = extract_eda(group, i, window_size, sampling_rate)

                combined = pd.concat([bvp, eda], axis=1)
                combined['video_id'] = video_id
                results.append(combined)
            except:
                continue
            
    features = pd.concat(results, ignore_index=True)
    final = pd.merge(features, annotations, on='video_id')
    final.to_csv(output_filename, index=False)
    return final

def extract_bvp(data, i, window_size, sampling_rate):
    bvp = data['BVP'][i:i+window_size]
    bvp, info = nk.ppg_process(bvp, sampling_rate=sampling_rate)
    bvp = nk.ppg_analyze(bvp, method="interval-related", sampling_rate=sampling_rate)
    bvp = bvp[['PPG_Rate_Mean', 'HRV_MeanNN', 'HRV_SDNN', 'HRV_RMSSD']]
    bvp = bvp.map(lambda x: x[0][0] if isinstance(x, np.ndarray) else x)
    return bvp

def extract_ecg(data, i, window_size, sampling_rate):
    ecg = data['ECG'][i:i+window_size]
    ecg, info = nk.ecg_process(ecg, sampling_rate=sampling_rate)
    ecg = nk.ecg_analyze(ecg, method="interval-related", sampling_rate=sampling_rate)
    ecg = ecg[['ECG_Rate_Mean', 'HRV_MeanNN', 'HRV_SDNN', 'HRV_RMSSD']]
    ecg = ecg.map(lambda x: x[0][0] if isinstance(x, np.ndarray) else x)
    return ecg

def extract_eda(data, i, window_size, sampling_rate):
    eda = data['EDA'][i:i+window_size]
    eda, info = nk.eda_process(eda, sampling_rate=sampling_rate)
    eda = nk.eda_analyze(eda, method="interval-related", sampling_rate=sampling_rate)
    eda = eda[['SCR_Peaks_N', 'SCR_Peaks_Amplitude_Mean', 'EDA_Tonic_SD']]
    return eda