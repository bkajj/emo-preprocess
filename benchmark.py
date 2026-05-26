from config import *
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.model_selection import GroupKFold, cross_validate, cross_val_predict, LeaveOneGroupOut
from sklearn.metrics import r2_score, mean_absolute_error

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 200)

MODELS = {
    'random_forest': RandomForestRegressor,
}

def get_cv(df):
    cv_cfg = config['cv']
    cv_type = cv_cfg['type']

    n_groups = df[cv_cfg['group_by']].nunique()
    if n_groups < 2:
        raise ValueError(f"Need at least 2 groups for CV, got {n_groups} for group_by={cv_cfg['group_by']}")
    
    if cv_type == 'group_kfold':
        n_splits = min(n_groups, cv_cfg['n_splits'])
        return GroupKFold(n_splits=n_splits)
    elif cv_type == 'leave_one_group_out':
        return LeaveOneGroupOut()
    else:
        raise ValueError(f"Unknown CV type: {cv_type}")

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

def create_model_from_config():
    model_cfg = config['model']
    model_class = MODELS[model_cfg['type']]
    return model_class(**model_cfg['params'])

def make_imputer(strategy):
    if strategy == 'impute_median':
        return SimpleImputer(strategy='median')
    elif strategy == 'impute_zero':
        return SimpleImputer(strategy='constant', fill_value=0)

def benchmark_datasets(processed):
    #correlation_check(processed)  
    #correlation_check_avg(processed)

    preprocessing_cfg = config['preprocessing']

    final_results = []
    for name, df in processed.items():

        if preprocessing_cfg['nan_handling'] == 'drop':
            df = df.dropna().reset_index(drop=True)
            model = create_model_from_config()
        else:
            imputer = make_imputer(preprocessing_cfg['nan_handling'])
            model = Pipeline([('imputer', imputer), ('regressor', create_model_from_config())])

        df = df.drop(columns=['video_id'], errors='ignore')

        feature_cols = df.drop(columns=['AROUSAL', 'VALENCE', 'SUBJECT_ID']).columns
        df = normalize_per_subject(df, feature_cols)
        
        X = df.drop(columns=['AROUSAL', 'VALENCE', 'SUBJECT_ID'])
        y = df[['VALENCE', 'AROUSAL']]

        #sanity_check(X, 'feature sanity check', name)
        #sanity_check(y, 'target sanity check', name)

        groups = df[config['cv']['group_by']]

        results = cross_validate(
            model, X, y, groups=groups, cv=get_cv(df), 
            scoring=['r2', 'neg_mean_squared_error', 'neg_mean_absolute_error'],
            return_train_score=True
            )
        
        preds =  cross_val_predict(model, X, y, groups=groups, cv=get_cv(df))
        r2_global_v = r2_score(y['VALENCE'], preds[:, 0])
        r2_global_a = r2_score(y['AROUSAL'], preds[:, 1])
        mae_global_v = mean_absolute_error(y['VALENCE'], preds[:, 0])
        mae_global_a = mean_absolute_error(y['AROUSAL'], preds[:, 1])

        final_results.append({
            'dataset': name,
            'r2_mean': results['test_r2'].mean(),
            'r2_std': results['test_r2'].std(),
            'mse_mean': -results['test_neg_mean_squared_error'].mean(),
            'mae_mean': -results['test_neg_mean_absolute_error'].mean(),
            'train_r2_mean': results['train_r2'].mean(),
            'r2_global_v': r2_global_v,
            'r2_global_a': r2_global_a,
            'mae_global_v': mae_global_v,
            'mae_global_a': mae_global_a,
        })
        
    return pd.DataFrame(final_results)