import fastf1
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from sklearn.preprocessing import StandardScaler
import numpy as np #nie masz czasu bawic sie w optymalizje
import matplotlib.pyplot as plt
import optuna
import seaborn as sns
from scipy.stats import ttest_rel
import shap
from xgboost import XGBRegressor
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense
from tensorflow.keras.optimizers import Adam



def load_data():
    global global_df
    fastf1.Cache.enable_cache('cache')
    session = fastf1.get_session(2023, 'Bahrain', 'R')
    session.load(telemetry=True)
    laps = session.laps
    df = pd.DataFrame(laps)
    global_df = df.copy()
    print(df.isnull().sum())
    df = df.dropna(subset=['LapTime', 'LapNumber', 'SpeedI1', 'SpeedI2', 'SpeedFL'])
    df['LapTime_sec'] = df['LapTime'].dt.total_seconds()
    X = df[['LapNumber', 'SpeedI1', 'SpeedI2', 'SpeedFL']]
    y = df['LapTime_sec']
    return X, y


X, y = load_data()
print(X.isnull().sum())
# Scale features
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)


X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=0.2, random_state=12)

# Linear Regression
linear_model = LinearRegression()
linear_model.fit(X_train, y_train)
y_pred_linear = linear_model.predict(X_test)
mse_linear = mean_squared_error(y_test, y_pred_linear)
print(f"Linear Regression MSE: {mse_linear:.3f}")

# Decision Tree
tree_model = DecisionTreeRegressor(random_state=12)
tree_model.fit(X_train, y_train)
y_pred_tree = tree_model.predict(X_test)
mse_tree = mean_squared_error(y_test, y_pred_tree)
print(f"Decision Tree MSE: {mse_tree:.3f}")


# Optuna
def objective(trial):
    max_depth = trial.suggest_int('max_depth', 2, 20)
    min_samples_split = trial.suggest_int('min_samples_split', 2, 20)
    min_samples_leaf = trial.suggest_int('min_samples_leaf', 1, 10)

    tree = DecisionTreeRegressor(
        random_state=12,
        max_depth=max_depth,
        min_samples_split=min_samples_split,
        min_samples_leaf=min_samples_leaf
    )
    tree.fit(X_train, y_train)
    y_pred = tree.predict(X_test)
    mse = mean_squared_error(y_test, y_pred)
    return mse


print("\nOptimizing Decision Tree with Optuna...")
study = optuna.create_study(direction="minimize")
study.optimize(objective, n_trials=30)

print("Best parameters:", study.best_params)
print(f"Best Decision Tree MSE: {study.best_value:.3f}")

# Retrain with best params
best_tree = DecisionTreeRegressor(random_state=12, **study.best_params)
best_tree.fit(X_train, y_train)
y_pred_best_tree = best_tree.predict(X_test)

# XGBoost Model
xgb_model = XGBRegressor(objective='reg:squarederror', random_state=12, n_estimators=100)
xgb_model.fit(X_train, y_train)
y_pred_xgb = xgb_model.predict(X_test)
mse_xgb = mean_squared_error(y_test, y_pred_xgb)
print(f"XGBoost MSE: {mse_xgb:.3f}")

# Keras Neural Network
keras_model = Sequential([
    Dense(16, activation='relu', input_shape=(X_train.shape[1],)),
    Dense(8, activation='relu'),
    Dense(1)
])
keras_model.compile(optimizer=Adam(learning_rate=0.01), loss='mse')
keras_model.fit(X_train, y_train, epochs=50, batch_size=16, verbose=0)
y_pred_keras = keras_model.predict(X_test).flatten()
mse_keras = mean_squared_error(y_test, y_pred_keras)
print(f"Keras Neural Net MSE: {mse_keras:.3f}")

# Example prediction
example = X_test[0]
print("\nExample input (scaled):")
print(example)

pred_linear = linear_model.predict([example])[0]
pred_tree = tree_model.predict([example])[0]
print(f"\nLinear Regression predicted lap time: {pred_linear:.3f} sec")
print(f"Decision Tree predicted lap time: {pred_tree:.3f} sec")
print(f"Actual lap time: {y_test.iloc[0]:.3f} sec")



results_df = X.copy()
results_df['LapNumber'] = X['LapNumber']
results_df['Actual'] = y.values
results_df['Predicted_Linear'] = linear_model.predict(scaler.transform(X))
results_df['Predicted_Tree'] = tree_model.predict(scaler.transform(X))
results_df['Predicted_OptunaTree'] = best_tree.predict(scaler.transform(X))

# srednie z okrazen
agg_df = results_df.groupby('LapNumber').agg({
    'Actual': 'mean',
    'Predicted_Linear': 'mean',
    'Predicted_Tree': 'mean',
    'Predicted_OptunaTree': 'mean'
}).reset_index()

# Przcinamy po IQR
Q1 = agg_df['Actual'].quantile(0.25)
Q3 = agg_df['Actual'].quantile(0.75)
IQR = Q3 - Q1
upper_bound = Q3 + 1.5 * IQR
lower_bound = Q1 - 1.5 * IQR
mask = (
        (agg_df['Actual'] <= upper_bound) & (agg_df['Actual'] >= lower_bound) &
        (agg_df['Predicted_Linear'] <= upper_bound) & (agg_df['Predicted_Linear'] >= lower_bound) &
        (agg_df['Predicted_Tree'] <= upper_bound) & (agg_df['Predicted_Tree'] >= lower_bound) &
        (agg_df['Predicted_OptunaTree'] <= upper_bound) & (agg_df['Predicted_OptunaTree'] >= lower_bound)
)
agg_clean = agg_df[mask]

# Melt to long format for seaborn
plot_long = pd.melt(
    agg_clean,
    id_vars=['LapNumber'],
    value_vars=['Actual', 'Predicted_Linear', 'Predicted_Tree', 'Predicted_OptunaTree'],
    var_name='Type',
    value_name='LapTime'
)

plt.figure(figsize=(14, 7))
# plot prawda
sns.scatterplot(
    data=plot_long[plot_long['Type'] == 'Actual'],
    x='LapNumber',
    y='LapTime',
    hue='Type',
    palette=['black'],
    s=60,
    alpha=1.0,
    edgecolor='w',
    legend=False
)
# Plot pred
sns.scatterplot(
    data=plot_long[plot_long['Type'] != 'Actual'],
    x='LapNumber',
    y='LapTime',
    hue='Type',
    palette=['blue', 'green', 'orange'],
    s=30,
    alpha=0.4,
    edgecolor='w'
)
# Add lines connecting the points for each type
for t, color in zip(['Actual', 'Predicted_Linear', 'Predicted_Tree', 'Predicted_OptunaTree'], ['black', 'blue', 'green', 'orange']):
    subset = plot_long[plot_long['Type'] == t].sort_values('LapNumber')
    plt.plot(subset['LapNumber'], subset['LapTime'], color=color, alpha=0.7, label=f'{t} (line)')
plt.xlabel("Lap Number")
plt.ylabel("Lap Time (seconds)")
plt.gca().invert_yaxis()
plt.suptitle("Actual vs Predicted Lap Times per Lap (Linear, Tree, Optuna Tree)")
plt.grid(
    color='lightgray',
    linestyle='--',
    alpha=0.7,
    which='both',
    axis='both'
)
sns.despine(left=False, bottom=False)
plt.tight_layout()
plt.show()

# --

driver1 = 'VER'
driver2 = 'LEC'


filtered = global_df.dropna(subset=['LapTime', 'LapNumber', 'SpeedI1', 'SpeedI2', 'SpeedFL', 'Driver']).copy()
filtered['LapTime_sec'] = filtered['LapTime'].dt.total_seconds()
filtered = filtered[filtered['Driver'].isin([driver1, driver2])].copy()

# Predict using models trained on the whole dataset
X_drivers = filtered[['LapNumber', 'SpeedI1', 'SpeedI2', 'SpeedFL']]
X_drivers_scaled = scaler.transform(X_drivers)
filtered['Predicted_Linear'] = linear_model.predict(X_drivers_scaled)
filtered['Predicted_Tree'] = tree_model.predict(X_drivers_scaled)
filtered['Predicted_OptunaTree'] = best_tree.predict(X_drivers_scaled)

# Group by LapNumber and Driver, take mean (in case of duplicates)
df_2drivers_agg = filtered.groupby(['LapNumber', 'Driver']).agg({
    'LapTime_sec': 'mean',
    'Predicted_Linear': 'mean',
    'Predicted_Tree': 'mean',
    'Predicted_OptunaTree': 'mean'
}).reset_index()

# Melt for plotting
plot_2drivers = pd.melt(
    df_2drivers_agg,
    id_vars=['LapNumber', 'Driver'],
    value_vars=['LapTime_sec', 'Predicted_Linear', 'Predicted_Tree', 'Predicted_OptunaTree'],
    var_name='Type',
    value_name='LapTime'
)

plt.figure(figsize=(15, 7))
for driver, color in zip([driver1, driver2], ['blue', 'red']):
    # Actual
    sns.lineplot(
        data=plot_2drivers[(plot_2drivers['Driver'] == driver) & (plot_2drivers['Type'] == 'LapTime_sec')],
        x='LapNumber', y='LapTime', label=f'{driver} Actual', color=color, linewidth=2)
    # Linear
    sns.lineplot(
        data=plot_2drivers[(plot_2drivers['Driver'] == driver) & (plot_2drivers['Type'] == 'Predicted_Linear')],
        x='LapNumber', y='LapTime', label=f'{driver} Linear', color=color, linestyle='--', alpha=0.6)
    # Tree
    sns.lineplot(
        data=plot_2drivers[(plot_2drivers['Driver'] == driver) & (plot_2drivers['Type'] == 'Predicted_Tree')],
        x='LapNumber', y='LapTime', label=f'{driver} Tree', color=color, linestyle=':', alpha=0.6)
    # Optuna Tree
    sns.lineplot(
        data=plot_2drivers[(plot_2drivers['Driver'] == driver) & (plot_2drivers['Type'] == 'Predicted_OptunaTree')],
        x='LapNumber', y='LapTime', label=f'{driver} OptunaTree', color=color, linestyle='-.', alpha=0.6)

plt.xlabel('Lap Number')
plt.ylabel('Lap Time (seconds)')
plt.gca().invert_yaxis()
plt.title(f'Actual vs Predicted Lap Times for {driver1} and {driver2} (Full Dataset Model)')
plt.grid(True, linestyle='--', alpha=0.7)
plt.legend()
plt.tight_layout()
plt.show()

# Statistical tests for model predictions
alpha = 0.05

test_linear_tree = ttest_rel(y_pred_linear, y_pred_tree)
test_linear_actual = ttest_rel(y_pred_linear, y_test)
test_tree_actual = ttest_rel(y_pred_tree, y_test)

def print_ttest(label, ttest):
    print(f"{label}: statistic = {ttest.statistic:.3f}, p-value = {ttest.pvalue:.4f}, df = {ttest.df}")

def interpret_ttest(test_result, label1, label2):
    if test_result.pvalue < alpha:
        print(f"The difference between {label1} and {label2} is statistically significant (p = {test_result.pvalue:.4f}).")
    else:
        print(f"No statistically significant difference between {label1} and {label2} (p = {test_result.pvalue:.4f}).")

print("\nStatistical tests (paired t-test):")
print_ttest("Linear vs Tree", test_linear_tree)
print_ttest("Linear vs Actual", test_linear_actual)
print_ttest("Tree vs Actual", test_tree_actual)

interpret_ttest(test_linear_tree, 'Linear Regression', 'Decision Tree')
interpret_ttest(test_linear_actual, 'Linear Regression', 'Actual')
interpret_ttest(test_tree_actual, 'Decision Tree', 'Actual')


# Feature importance for Linear Regression
print("\nLinear Regression feature importances (coefficients):")
for name, coef in zip(X.columns, linear_model.coef_):
    print(f"{name}: {coef:.4f}")

# Feature importance for Decision Tree
print("\nDecision Tree feature importances:")
for name, imp in zip(X.columns, tree_model.feature_importances_):
    print(f"{name}: {imp:.4f}")

# SHAP values for best_tree (Optuna)
try:
    X_test_df = pd.DataFrame(X_test, columns=X.columns)
    

    explainer = shap.TreeExplainer(best_tree)
    shap_values = explainer.shap_values(X_test_df)
    shap.summary_plot(shap_values, X_test_df, feature_names=X.columns)
    
    shap.plots.waterfall(shap.Explanation(
        values=shap_values[0], 
        base_values=explainer.expected_value, 
        data=X_test_df.iloc[0], 
        feature_names=X.columns.tolist()
    ))
except Exception as e:
    print(f"SHAP summary plot failed: {e}")

# Model efficiency plots
plt.figure(figsize=(10, 6))
plt.scatter(y_test, y_pred_linear, alpha=0.5, label='Linear Regression', color='blue')
plt.scatter(y_test, y_pred_tree, alpha=0.5, label='Decision Tree', color='green')
plt.scatter(y_test, y_pred_best_tree, alpha=0.5, label='Optuna Tree', color='orange')
plt.scatter(y_test, y_pred_xgb, alpha=0.5, label='XGBoost', color='purple')
plt.scatter(y_test, y_pred_keras, alpha=0.5, label='Keras NN', color='red')
plt.xlabel('Actual Lap Time (s)')
plt.ylabel('Predicted Lap Time (s)')
plt.title('Actual vs Predicted Lap Times (Test Set)')
plt.legend()
plt.tight_layout()
plt.show()

plt.figure(figsize=(10, 6))
plt.hist(y_test - y_pred_linear, bins=30, alpha=0.5, label='Linear Regression', color='blue')
plt.hist(y_test - y_pred_tree, bins=30, alpha=0.5, label='Decision Tree', color='green')
plt.hist(y_test - y_pred_best_tree, bins=30, alpha=0.5, label='Optuna Tree', color='orange')
plt.hist(y_test - y_pred_xgb, bins=30, alpha=0.5, label='XGBoost', color='purple')
plt.hist(y_test - y_pred_keras, bins=30, alpha=0.5, label='Keras NN', color='red')
plt.xlabel('Prediction Error (s)')
plt.ylabel('Frequency')
plt.title('Distribution of Prediction Errors')
plt.legend()
plt.tight_layout()
plt.show()
