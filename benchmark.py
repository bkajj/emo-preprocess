from config import *
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, root_mean_squared_error, r2_score
from sklearn.preprocessing import StandardScaler

def measure_model(model, y_test, pred):
    results = {
        'model': model,
        'MAE_valence': mean_absolute_error(y_test["VALENCE"], pred[:, 0]),
        'MAE_arousal': mean_absolute_error(y_test["AROUSAL"], pred[:, 1]),
        'RMSE_valence': root_mean_squared_error(y_test["VALENCE"], pred[:, 0]),
        'RMSE_arousal': root_mean_squared_error(y_test["AROUSAL"], pred[:, 1]),
        'R2_valence': r2_score(y_test["VALENCE"], pred[:, 0]),
        'R2_arousal': r2_score(y_test["AROUSAL"], pred[:, 1]),
    }
    
    return pd.DataFrame([results])

def benchmark_datasets(processed):
    results = []
    for name, df in processed.items():
        df = df.fillna(0)
        df = df.drop(columns=['video_id', 'Unnamed: 0'], errors='ignore')
        
        X = df.drop(columns=['AROUSAL', 'VALENCE'])
        y = df[['VALENCE', 'AROUSAL']]
        
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        scaler = StandardScaler()
        X_train = scaler.fit_transform(X_train)
        X_test = scaler.transform(X_test)
        
        model = RandomForestRegressor(n_estimators=100)
        
        model.fit(X_train, y_train)
        pred = model.predict(X_test)
        results.append(measure_model(name, y_test, pred))
        
    results = pd.concat(results, ignore_index=True)
    return results