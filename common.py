from config import *
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.model_selection import GroupKFold, cross_validate, cross_val_predict, LeaveOneGroupOut
from sklearn.metrics import r2_score, mean_absolute_error

def get_cv(df, variant):
    cv_cfg = config['cv'][variant]
    cv_type = cv_cfg['type']

    if cv_type == 'group_kfold':
        return GroupKFold(n_splits=cv_cfg['n_splits'])
    elif cv_type == 'leave_one_group_out':
        return LeaveOneGroupOut()
    else:
        raise ValueError(f"Unknown CV type: {cv_type}")
    
def normalize_per_subject(df, feature_cols):
    df = df.copy()
    for sub_id, group in df.groupby('SUBJECT_ID'):
        means = group[feature_cols].mean()
        stds = group[feature_cols].std().replace(0, 1)
        df.loc[group.index, feature_cols] = (group[feature_cols] - means) / stds
    return df

def make_imputer(strategy):
    if strategy == 'impute_median':
        return SimpleImputer(strategy='median')
    elif strategy == 'impute_zero':
        return SimpleImputer(strategy='constant', fill_value=0)
    
def sanity_check(df, title, dataset_name):
    print(f'{dataset_name}: {title}\n{df.describe()}')