import pandas as pd
import numpy as np
import neurokit2 as nk
import warnings;
warnings.filterwarnings('ignore')
from config import *

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