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

def benchmark_datasets(processed):

    df_deap = processed['deap']
    print(df_deap[['VALENCE', 'AROUSAL']].value_counts().head(20))
    print(df_deap['VALENCE'].isna().sum())

    final_results = []
    for name, df in processed.items():
        df = df.dropna().reset_index(drop=True)
        df = df.drop(columns=['video_id'], errors='ignore')
        
        X = df.drop(columns=['AROUSAL', 'VALENCE', 'SUBJECT_ID'])
        y = df[['VALENCE', 'AROUSAL']]

        sanity_check(X, 'feature sanity check', name)
        sanity_check(y, 'target sanity check', name)

        groups = df['SUBJECT_ID']
        
        model = RandomForestRegressor(n_estimators=100, random_state=42)

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