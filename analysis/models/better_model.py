import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.preprocessing import StandardScaler, LabelEncoder
import optuna
import shap
from xgboost import XGBRegressor
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Dropout
from tensorflow.keras.optimizers import Adam
from tensorflow.keras.callbacks import EarlyStopping

def convert_timedelta_to_seconds(df):
    for col in df.columns:
        if np.issubdtype(df[col].dtype, np.timedelta64):
            df[col] = df[col].dt.total_seconds()
    return df

# Load and prepare data
season_df = pd.read_pickle('season_full.pkl')
season_df = season_df.dropna(subset=['LapTime'])
season_df = convert_timedelta_to_seconds(season_df)

X = season_df[['Compound', 'TyreLife', 'StartFuel', 'SpeedI1', 'SpeedI2', 'SpeedFL', 
               'SumLonAcc', 'SumLatAcc', 'MeanLapSpeed', 'LonDistanceDTW', 'LatDistanceDTW']]
y = season_df['LapTime']


le = LabelEncoder()
X.loc[:, 'Compound'] = le.fit_transform(X['Compound'])

# Remove inf/-inf and NaN values
X = X.replace([np.inf, -np.inf], np.nan)
X = X.dropna()
y = y.loc[X.index]

scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)
X_scaled = pd.DataFrame(X_scaled, columns=X.columns)


X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=0.2, random_state=42)


def objective_xgb(trial):
    params = {
        'max_depth': trial.suggest_int('max_depth', 3, 10),
        'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.3),
        'n_estimators': trial.suggest_int('n_estimators', 50, 300),
        'min_child_weight': trial.suggest_int('min_child_weight', 1, 7),
        'subsample': trial.suggest_float('subsample', 0.6, 1.0),
        'colsample_bytree': trial.suggest_float('colsample_bytree', 0.6, 1.0),
        'gamma': trial.suggest_float('gamma', 0, 5)
    }
    
    model = XGBRegressor(**params, random_state=42)
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)
    return mean_squared_error(y_test, y_pred)

# Optimize Keras with Optuna
def objective_keras(trial):
    model = Sequential()
    n_layers = trial.suggest_int('n_layers', 2, 5)
    
    # Input layer
    model.add(Dense(
        trial.suggest_int(f'n_units_0', 32, 256),
        activation=trial.suggest_categorical('activation_0', ['relu', 'elu']),
        input_shape=(X_train.shape[1],)
    ))
    model.add(Dropout(trial.suggest_float(f'dropout_0', 0.1, 0.5)))
    
    # Hidden layers
    for i in range(1, n_layers):
        model.add(Dense(
            trial.suggest_int(f'n_units_{i}', 16, 128),
            activation=trial.suggest_categorical(f'activation_{i}', ['relu', 'elu'])
        ))
        model.add(Dropout(trial.suggest_float(f'dropout_{i}', 0.1, 0.5)))
    
    # Output layer
    model.add(Dense(1))
    
    model.compile(
        optimizer=Adam(learning_rate=trial.suggest_float('learning_rate', 1e-4, 1e-2)),
        loss='mse'
    )
    
    early_stopping = EarlyStopping(monitor='val_loss', patience=10, restore_best_weights=True)
    
    history = model.fit(
        X_train, y_train,
        validation_split=0.2,
        epochs=100,
        batch_size=trial.suggest_categorical('batch_size', [16, 32, 64, 128]),
        callbacks=[early_stopping],
        verbose=0
    )
    
    return min(history.history['val_loss'])

# Optimize XGBoost
print("\nOptimizing XGBoost...")
study_xgb = optuna.create_study(direction="minimize")
study_xgb.optimize(objective_xgb, n_trials=50)

# Train best XGBoost model
best_xgb = XGBRegressor(**study_xgb.best_params, random_state=42)
best_xgb.fit(X_train, y_train)
y_pred_xgb = best_xgb.predict(X_test)

# Optimize Keras
print("\nOptimizing Keras model...")
study_keras = optuna.create_study(direction="minimize")
study_keras.optimize(objective_keras, n_trials=50)

# Train best Keras model
best_keras = Sequential()
n_layers = study_keras.best_params['n_layers']

best_keras.add(Dense(
    study_keras.best_params['n_units_0'],
    activation=study_keras.best_params['activation_0'],
    input_shape=(X_train.shape[1],)
))
best_keras.add(Dropout(study_keras.best_params['dropout_0']))

for i in range(1, n_layers):
    best_keras.add(Dense(
        study_keras.best_params[f'n_units_{i}'],
        activation=study_keras.best_params[f'activation_{i}']
    ))
    best_keras.add(Dropout(study_keras.best_params[f'dropout_{i}']))

best_keras.add(Dense(1))
best_keras.compile(optimizer=Adam(learning_rate=study_keras.best_params['learning_rate']), loss='mse')

early_stopping = EarlyStopping(monitor='val_loss', patience=10, restore_best_weights=True)
history = best_keras.fit(
    X_train, y_train,
    validation_split=0.2,
    epochs=100,
    batch_size=study_keras.best_params['batch_size'],
    callbacks=[early_stopping],
    verbose=0
)

y_pred_keras = best_keras.predict(X_test).flatten()

# Print model performances
print("\nModel Performances:")
print(f"XGBoost MSE: {mean_squared_error(y_test, y_pred_xgb):.4f}")
print(f"XGBoost R2: {r2_score(y_test, y_pred_xgb):.4f}")
print(f"Keras MSE: {mean_squared_error(y_test, y_pred_keras):.4f}")
print(f"Keras R2: {r2_score(y_test, y_pred_keras):.4f}")

# --- Compare to actual laps of one track ---
track_name = "Bahrain"  # Change to your desired track name

# Get the indices in the test set that correspond to the chosen track
test_indices = y_test.index
track_mask = season_df.loc[test_indices, 'Track'] == track_name

y_test_track = y_test[track_mask]
y_pred_xgb_track = y_pred_xgb[track_mask]
y_pred_keras_track = y_pred_keras[track_mask]

plt.figure(figsize=(10, 5))
plt.scatter(y_test_track, y_pred_xgb_track, alpha=0.5, label='XGBoost')
plt.scatter(y_test_track, y_pred_keras_track, alpha=0.5, label='Keras')
plt.plot([y_test_track.min(), y_test_track.max()], [y_test_track.min(), y_test_track.max()], 'r--', lw=2)
plt.xlabel('Actual Lap Time')
plt.ylabel('Predicted Lap Time')
plt.title(f'Actual vs Predicted Lap Times for {track_name}')
plt.legend()
plt.tight_layout()
plt.show()

print(f"{track_name} XGBoost MSE: {mean_squared_error(y_test_track, y_pred_xgb_track):.4f}")
print(f"{track_name} XGBoost R2: {r2_score(y_test_track, y_pred_xgb_track):.4f}")
print(f"{track_name} Keras MSE: {mean_squared_error(y_test_track, y_pred_keras_track):.4f}")
print(f"{track_name} Keras R2: {r2_score(y_test_track, y_pred_keras_track):.4f}")

# Plot prediction errors distribution
plt.figure(figsize=(12, 6))
plt.subplot(1, 2, 1)
plt.hist(y_test - y_pred_xgb, bins=50, alpha=0.7)
plt.xlabel('Prediction Error')
plt.ylabel('Frequency')
plt.title('XGBoost Error Distribution')

plt.subplot(1, 2, 2)
plt.hist(y_test - y_pred_keras, bins=50, alpha=0.7)
plt.xlabel('Prediction Error')
plt.ylabel('Frequency')
plt.title('Keras Error Distribution')
plt.tight_layout()
plt.show()

# Plot feature importance for XGBoost
plt.figure(figsize=(10, 6))
feature_importance = pd.DataFrame({
    'feature': X.columns,
    'importance': best_xgb.feature_importances_
}).sort_values('importance', ascending=True)

plt.barh(range(len(feature_importance)), feature_importance['importance'])
plt.yticks(range(len(feature_importance)), feature_importance['feature'])
plt.xlabel('Feature Importance')
plt.title('XGBoost Feature Importance')
plt.tight_layout()
plt.show()

# SHAP values for XGBoost
explainer = shap.TreeExplainer(best_xgb)
shap_values = explainer.shap_values(X_test)
plt.figure(figsize=(12, 8))
shap.summary_plot(shap_values, X_test, feature_names=X.columns)
plt.tight_layout()
plt.show()

# Plot training history for Keras
plt.figure(figsize=(10, 6))
plt.plot(history.history['loss'], label='Training Loss')
plt.plot(history.history['val_loss'], label='Validation Loss')
plt.xlabel('Epoch')
plt.ylabel('Loss')
plt.title('Keras Model Training History')
plt.legend()
plt.tight_layout()
plt.show()




