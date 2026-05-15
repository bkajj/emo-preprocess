from config import *
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import GroupKFold, cross_validate

def get_cv(df):
    n_subjects = df['SUBJECT_ID'].nunique()
    if n_subjects < 2:
        raise ValueError("Min subject count = 2")
    n_splits = min(n_subjects, 5)
    return GroupKFold(n_splits=n_splits)

def sanity_check(df, title, dataset_name):
    print(f'{dataset_name}: {title}\n{df.describe()}')

def normalize_per_subject(df, feature_cols):
    df = df.copy()
    for sub_id, group in df.groupby('SUBJECT_ID'):
        means = group[feature_cols].mean()
        stds = group[feature_cols].std().replace(0, 1)
        df.loc[group.index, feature_cols] = (group[feature_cols] - means) / stds
    return df

def correlation_check(processed):
    """Pokazuje korelacje cechy ↔ targety dla 1 wybranego badanego z każdego datasetu."""
    for name, df in processed.items():
        df = df.dropna().reset_index(drop=True)

        sub_id = df.groupby('SUBJECT_ID').size().idxmax()
        sub_df = df[df['SUBJECT_ID'] == sub_id]
        
        feature_cols = sub_df.drop(columns=['AROUSAL', 'VALENCE', 'SUBJECT_ID', 'video_id'], errors='ignore').columns
        
        corr = sub_df[list(feature_cols) + ['VALENCE', 'AROUSAL']].corr()[['VALENCE', 'AROUSAL']]
        corr = corr.drop(['VALENCE', 'AROUSAL'])  # nie pokazuj korelacji target-target
        
        print(f"\n=== {name} (subject: {sub_id}, n_windows: {len(sub_df)}) ===")
        print(corr.round(3))

def correlation_check_avg(processed):
    for name, df in processed.items():
        df = df.dropna().reset_index(drop=True)
        feature_cols = df.drop(columns=['AROUSAL', 'VALENCE', 'SUBJECT_ID', 'video_id'], errors='ignore').columns
        
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

def benchmark_datasets(processed):
    #correlation_check(processed)  
    correlation_check_avg(processed)

    final_results = []
    for name, df in processed.items():
        df = df.dropna().reset_index(drop=True)
        df = df.drop(columns=['video_id'], errors='ignore')

        feature_cols = df.drop(columns=['AROUSAL', 'VALENCE', 'SUBJECT_ID']).columns
        df = normalize_per_subject(df, feature_cols)
        
        X = df.drop(columns=['AROUSAL', 'VALENCE', 'SUBJECT_ID'])
        y = df[['VALENCE', 'AROUSAL']]

        sanity_check(X, 'feature sanity check', name)
        sanity_check(y, 'target sanity check', name)

        groups = df['SUBJECT_ID']
        
        model = RandomForestRegressor(
            n_estimators=200,
            max_depth=4,           
            min_samples_leaf=30,   
            random_state=42
        )

        results = cross_validate(
            model, X, y, groups=groups, cv=get_cv(df), 
            scoring=['r2', 'neg_mean_squared_error', 'neg_mean_absolute_error'],
            return_train_score=True
            )
        
        final_results.append({
            'dataset': name,
            'r2_mean': results['test_r2'].mean(),
            'r2_std': results['test_r2'].std(),
            'mse_mean': -results['test_neg_mean_squared_error'].mean(),
            'mae_mean': -results['test_neg_mean_absolute_error'].mean(),
            'train_r2_mean': results['train_r2'].mean(),
        })
        
    return pd.DataFrame(final_results)