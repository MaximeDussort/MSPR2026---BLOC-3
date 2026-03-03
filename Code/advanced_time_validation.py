# advanced_time_validation.py

import pandas as pd
import numpy as np
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.ensemble import RandomForestRegressor
import matplotlib.pyplot as plt

def walk_forward_validation(df, target_col, date_col):

    df = df.sort_values(date_col)
    df["year"] = pd.to_datetime(df[date_col]).dt.year

    years = sorted(df["year"].unique())
    results = []

    for i in range(5, len(years)-1):
        train_years = years[:i]
        test_year = years[i]

        train = df[df["year"].isin(train_years)]
        test = df[df["year"] == test_year]

        X_train = train.drop(columns=[target_col, date_col])
        y_train = train[target_col]

        X_test = test.drop(columns=[target_col, date_col])
        y_test = test[target_col]

        model = RandomForestRegressor(n_estimators=200)
        model.fit(X_train, y_train)

        preds = model.predict(X_test)

        mae = mean_absolute_error(y_test, preds)
        rmse = np.sqrt(mean_squared_error(y_test, preds))

        results.append({
            "test_year": test_year,
            "MAE": mae,
            "RMSE": rmse
        })

        print(f"Année test {test_year} | MAE={mae:.2f} | RMSE={rmse:.2f}")

    return pd.DataFrame(results)