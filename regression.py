from config import *
from common import make_imputer, normalize_per_subject, get_cv
from eda import correlation_check, correlation_check_avg
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.pipeline import Pipeline
from sklearn.model_selection import cross_validate, cross_val_predict
from sklearn.metrics import r2_score, mean_absolute_error
from sklearn.base import clone

MODELS = {
    'random_forest': RandomForestRegressor,
}

def create_model_from_config(variant):
    model_cfg = config['model'][variant]
    model_class = MODELS[model_cfg['type']]
    return model_class(**model_cfg['params'])

def avg_metrics_per_fold(result):
    return {
        'r2_mean': result['test_r2'].mean(),
        'r2_std': result['test_r2'].std(),
        'mse_mean': -result['test_neg_mean_squared_error'].mean(),
        'mae_mean': -result['test_neg_mean_absolute_error'].mean(),
        'train_r2_mean': result['train_r2'].mean()
    }

def avg_metrics_per_subject(preds, y):
    return {
        'r2_global_v': r2_score(y['VALENCE'], preds[:, 0]),
        'r2_global_a': r2_score(y['AROUSAL'], preds[:, 1]),
        'mae_global_v': mean_absolute_error(y['VALENCE'], preds[:, 0]),
        'mae_global_a': mean_absolute_error(y['AROUSAL'], preds[:, 1])
    }

def measure_model_indep(y, preds, results, metrics, name):
    r2_global_v = r2_score(y['VALENCE'], preds[:, 0])
    r2_global_a = r2_score(y['AROUSAL'], preds[:, 1])
    mae_global_v = mean_absolute_error(y['VALENCE'], preds[:, 0])
    mae_global_a = mean_absolute_error(y['AROUSAL'], preds[:, 1])

    metrics.append({
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
    
    return metrics

def evaluate_model_subject_independent(processed):
    #correlation_check(processed)  
    #correlation_check_avg(processed)

    preprocessing_cfg = config['preprocessing']

    metrics = []
    for name, df in processed.items():

        if preprocessing_cfg['nan_handling'] == 'drop':
            df = df.dropna().reset_index(drop=True)
            model = Pipeline([('regressor', create_model_from_config('subject_independent'))])
        else:
            imputer = make_imputer(preprocessing_cfg['nan_handling'])
            model = Pipeline([('imputer', imputer), ('regressor', create_model_from_config('subject_independent'))])

        # no leakage, because we normalize per subject and cv splits also by subjects
        X = df.drop(columns=['AROUSAL', 'VALENCE', 'STIMULI_ID'])
        if preprocessing_cfg['subject_independent']['normalization'] == 'per_subject_zscore':
            X = normalize_per_subject(X, X.columns.drop('SUBJECT_ID'))
        X = X.drop(columns=['SUBJECT_ID'])
        
        y = df[['VALENCE', 'AROUSAL']]

        groups = df['SUBJECT_ID']
        cv = get_cv(df, 'subject_independent')

        result = cross_validate(
            model, X, y, groups=groups, cv=cv, 
            scoring=['r2', 'neg_mean_squared_error', 'neg_mean_absolute_error'],
            return_train_score=True
        )

        ##### feature importance
        model_v = clone(model)
        model_v.fit(X, y['VALENCE'])
        fi_v = model_v.named_steps['regressor'].feature_importances_
        feature_importance_valence = pd.Series(fi_v, index=X.columns)

        model_a = clone(model)
        model_a.fit(X, y['AROUSAL'])
        fi_a = model_a.named_steps['regressor'].feature_importances_
        feature_importance_arousal = pd.Series(fi_a, index=X.columns)

        corr_v = X.corrwith(y['VALENCE'])
        corr_a = X.corrwith(y['AROUSAL'])

        feature_analysis = pd.DataFrame({
            'corr_v': corr_v,
            'imp_v': feature_importance_valence,
            'corr_a': corr_a,
            'imp_a': feature_importance_arousal,
        })
        print(f"{name}: feature analysis - subject independent")
        print(feature_analysis.round(3))

        preds = cross_val_predict(model, X, y, groups=groups, cv=cv)

        metrics = measure_model_indep(y, preds, result, metrics, name)

    #sanity_check(X, 'feature sanity check', name)
    #sanity_check(y, 'target sanity check', name)

    return pd.DataFrame(metrics)

def evaluate_model_subject_dependent(processed):
    correlation_check(processed)  
    correlation_check_avg(processed)

    preprocessing_cfg = config['preprocessing']

    metrics = {}
    for name, df in processed.items():

        if preprocessing_cfg['nan_handling'] == 'drop':
            df = df.dropna().reset_index(drop=True)
            model = Pipeline([('regressor', create_model_from_config('subject_dependent'))])
        else:
            imputer = make_imputer(preprocessing_cfg['nan_handling'])
            model = Pipeline([('imputer', imputer), ('regressor', create_model_from_config('subject_dependent'))])

        metrics_per_fold_in_subject = {}
        metrics_per_subject = {}
        feature_importances = {}
        df_per_subject = df.groupby('SUBJECT_ID')
        for sub, sub_df in df_per_subject:

            print(f'{name}: eval {sub}')

            if sub_df['STIMULI_ID'].nunique() < config['cv']['subject_dependent']['n_splits']:
                continue

            X = sub_df.drop(columns=['AROUSAL', 'VALENCE', 'SUBJECT_ID', 'STIMULI_ID'])
            y = sub_df[['VALENCE', 'AROUSAL']]

            groups = sub_df['STIMULI_ID']
            cv = get_cv(sub_df, 'subject_dependent')
            
            # metrics per fold (group of stimuli or single stimuli (LOSO))
            sub_result = cross_validate(
                model, X, y, groups=groups, cv=cv, 
                scoring=['r2', 'neg_mean_squared_error', 'neg_mean_absolute_error'],
                return_train_score=True
            )
            #feature_importances[sub] = model.feature_importances_

            # results (predictions) per subject
            preds = cross_val_predict(model, X, y, groups=groups, cv=cv)

            if name == 'case':
                compare = pd.DataFrame({
                'true_v': y['VALENCE'].values,
                'pred_v': preds[:, 0],
                'true_a': y['AROUSAL'].values,
                'pred_a': preds[:, 1],
                })
                print(f'{sub}:\n{compare.describe()}')

            # calculate average metrics per fold, diagnostic useful for overfitting detection (train_r2 vs test_r2)
            metrics_per_fold_in_subject[sub] = avg_metrics_per_fold(sub_result)

            # calculate average metrics for whole subject
            metrics_per_subject[sub] = avg_metrics_per_subject(preds, y)

        # feature_importances_df = pd.DataFrame.from_dict(feature_importances, orient='index')
        # feature_importances_stats = feature_importances_df.describe()
        # print(f"{name}: feature importance - subject dependent")
        # print(feature_importances_stats)
        # print(feature_importances_df)

        metrics_per_subject_df = pd.DataFrame.from_dict(metrics_per_subject, orient='index')
        print(f"{name}: rozklad metryk")
        print(f"Ile badanych r2 >0: {(metrics_per_subject_df > 0).sum()}")
        print(metrics_per_subject_df.describe())
        print(metrics_per_subject_df.sort_values('r2_global_a'))
        
        metrics_per_subject_avg = metrics_per_subject_df.mean()
        
        metrics_per_fold_subject_df = pd.DataFrame.from_dict(metrics_per_fold_in_subject, orient='index')
        metrics_per_fold_subject_avg = metrics_per_fold_subject_df.mean()

        metrics[name] = pd.concat([metrics_per_subject_avg, metrics_per_fold_subject_avg])

    #sanity_check(X, 'feature sanity check', name)
    #sanity_check(y, 'target sanity check', name)

    return pd.DataFrame(metrics)