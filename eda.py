from config import *
import pandas as pd

def correlation_check(processed):
    """Pokazuje korelacje cechy ↔ targety dla 1 wybranego badanego z każdego datasetu."""
    for name, df in processed.items():
        df = df.dropna().reset_index(drop=True)

        sub_id = df.groupby('SUBJECT_ID').size().idxmax()
        sub_df = df[df['SUBJECT_ID'] == sub_id]
        
        feature_cols = sub_df.drop(columns=['AROUSAL', 'VALENCE', 'SUBJECT_ID', 'STIMULI_ID'], errors='ignore').columns
        
        corr = sub_df[list(feature_cols) + ['VALENCE', 'AROUSAL']].corr()[['VALENCE', 'AROUSAL']]
        corr = corr.drop(['VALENCE', 'AROUSAL'])  # nie pokazuj korelacji target-target
        
        print(f"\n=== {name} (subject: {sub_id}, n_windows: {len(sub_df)}) ===")
        print(corr.round(3))

def correlation_check_avg(processed):
    for name, df in processed.items():
        df = df.dropna().reset_index(drop=True)
        feature_cols = df.drop(columns=['AROUSAL', 'VALENCE', 'SUBJECT_ID', 'STIMULI_ID'], errors='ignore').columns
        
        corrs_per_subject = []
        for sub_id, group in df.groupby('SUBJECT_ID'):
            if len(group) < 20:
                continue
            corr = group[list(feature_cols) + ['VALENCE', 'AROUSAL']].corr()[['VALENCE', 'AROUSAL']]
            corr = corr.drop(['VALENCE', 'AROUSAL'])
            corrs_per_subject.append(corr)
        
        if not corrs_per_subject:
            print(f"\n=== {name}: za mało badanych z >20 oknami ===")
            continue
            
        all_corrs = pd.concat(corrs_per_subject)
        avg = all_corrs.groupby(level=0).mean()
        std = all_corrs.groupby(level=0).std()
        
        print(f"\n=== {name} (n={len(corrs_per_subject)} subjects) ===")
        print("Mean correlations across subjects:")
        print(avg.round(3))
        print("\nStd of correlations (jak bardzo wartości różnią się między badanymi):")
        print(std.round(3))

