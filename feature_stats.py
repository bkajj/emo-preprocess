import pandas as pd
import matplotlib.pyplot as plt
import sys
import os

def main(result_path):
    datasets = ['deap', 'case', 'biraffe']
    features = ['ECG_Rate_Mean', 'HRV_MeanNN', 'HRV_SDNN', 'HRV_RMSSD', 'SCR_Peaks_N', 'SCR_Peaks_Amplitude_Mean', 'EDA_Tonic_SD']
    features_deap = ['PPG_Rate_Mean', 'HRV_MeanNN', 'HRV_SDNN', 'HRV_RMSSD', 'SCR_Peaks_N', 'SCR_Peaks_Amplitude_Mean', 'EDA_Tonic_SD']

    for d in datasets:
        df = pd.read_csv(os.path.join(result_path, d.upper() + '.csv'))
        if d == 'deap':
            df = df[features_deap]
        else:
            df = df[features]
        print(d)
        print(df.describe())
        #print(df.describe())
        print()

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Specify result folder path in args")
        sys.exit(1)

    main(sys.argv[1])