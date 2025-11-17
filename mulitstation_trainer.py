from __future__ import annotations
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Dict, Any
from pandas.api.types import is_numeric_dtype

from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score


# =====================================================================
# 1. MODEL FACTORY
# =====================================================================
def make_model():
    """
    Create and return a machine-learning classifier for heavy rain prediction.
    Priority:
        1. LightGBM (best performance)
        2. XGBoost (fallback)
        3. RandomForest (fallback)

    Returns
    -------
    model : classifier implementing fit() and predict()
    """
    try:
        import lightgbm as lgb
        print("Using LightGBM")
        return lgb.LGBMClassifier(
            n_estimators=300,
            learning_rate=0.05,
            subsample=0.9,
            colsample_bytree=0.9,
            random_state=42,
        )
    except Exception:
        try:
            import xgboost as xgb
            print("Using XGBoost")
            return xgb.XGBClassifier(
                n_estimators=300,
                learning_rate=0.05,
                subsample=0.9,
                colsample_bytree=0.9,
                max_depth=6,
                random_state=42,
                eval_metric="logloss",
            )
        except Exception:
            from sklearn.ensemble import RandomForestClassifier
            print("Using RandomForest")
            return RandomForestClassifier(
                n_estimators=200,
                max_depth=12,
                n_jobs=-1,
                random_state=42,
            )


# =====================================================================
# 2. FEATURE ENGINEERING
# =====================================================================
def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build all engineered features used in multi-day heavy-rain prediction.

    Includes:
        • Numeric cleaning
        • Time-based sorting per station
        • Lag features (1, 2, 3, 5, 7 days)
        • Rolling rainfall features (sum/mean for 3-day and 7-day)
        • Calendar features (year, month, day-of-year)
        • Seasonal cyclic features (sin/cos DOY)
        • One-hot encoded station identifiers

    Parameters
    ----------
    df : pd.DataFrame
        Raw merged dataset from multiple hydrometric stations.

    Returns
    -------
    pd.DataFrame
        Fully engineered feature matrix.
    """
    df = df.copy()

    # Handle dates safely
    df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce")
    df = df.dropna(subset=["Fecha"])

    # Ensure rainfall is numeric
    df["Precipitacion_filled"] = (
        pd.to_numeric(df["Precipitacion_filled"], errors="coerce").fillna(0)
    )

    # Sort by station + date (VERY IMPORTANT for lag features)
    df = df.sort_values(["Estacion", "Fecha"]).reset_index(drop=True)

    # Group rainfall per station
    g = df.groupby("Estacion")["Precipitacion_filled"]

    # --- Lag features ---
    for lag in [1, 2, 3, 5, 7]:
        df[f"precip_lag{lag}"] = g.shift(lag)

    # --- Rolling window features ---
    df["precip_roll3_sum"] = g.shift(1).rolling(3).sum()
    df["precip_roll7_sum"] = g.shift(1).rolling(7).sum()
    df["precip_roll3_mean"] = g.shift(1).rolling(3).mean()
    df["precip_roll7_mean"] = g.shift(1).rolling(7).mean()

    # --- Calendar features ---
    df["year"] = df["Fecha"].dt.year
    df["month"] = df["Fecha"].dt.month
    df["dayofyear"] = df["Fecha"].dt.dayofyear

    # Cyclical seasonal encoding
    df["sin_doy"] = np.sin(2 * np.pi * df["dayofyear"] / 365.25)
    df["cos_doy"] = np.cos(2 * np.pi * df["dayofyear"] / 365.25)

    # One-hot encode station column
    df = pd.get_dummies(df, columns=["Estacion"], drop_first=True)

    # --- Fill remaining numeric NaNs safely ---
    for col in df.columns:
        if col not in ["HR_1d", "HR_2d", "HR_3d"] and is_numeric_dtype(df[col]):
            df[col] = df[col].fillna(0)

    return df


# =====================================================================
# 3. TRAINING + EVALUATION
# =====================================================================
def train_and_eval(df: pd.DataFrame, target_col: str) -> Dict[str, Any]:
    """
    Train a model for a given prediction horizon and compute performance metrics.

    Parameters
    ----------
    df : DataFrame
        Feature-engineered dataset.
    target_col : str
        One of {"HR_1d", "HR_2d", "HR_3d"}.

    Returns
    -------
    dict
        Dictionary with accuracy, precision, recall, F1, and class counts.
    """
    # Time-sorted dataset for forecasting correctness
    df_sorted = df.sort_values("Fecha").reset_index(drop=True)

    # Extract features (keep DataFrame to avoid LightGBM warnings)
    feature_cols = [
        c for c in df_sorted.columns
        if c not in ["Fecha", "HR_1d", "HR_2d", "HR_3d"]
    ]
    X = df_sorted[feature_cols]
    y = df_sorted[target_col].astype(int)

    # Time-based split (80% train, 20% test)
    split_idx = int(0.8 * len(df_sorted))
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

    # Train model
    model = make_model()
    model.fit(X_train, y_train)

    # Predict using DataFrames (no warnings)
    y_pred = model.predict(X_test)

    # Safe conversion to numpy ints
    y_pred = np.asarray(y_pred).astype(int)
    y_test = np.asarray(y_test).astype(int)

    # Return metrics
    return {
        "target": target_col,
        "positives_train": int(y_train.sum()),
        "positives_test": int(y_test.sum()),
        "test_size": len(y_test),
        "accuracy": accuracy_score(y_test, y_pred),
        "precision_pos": precision_score(y_test, y_pred, zero_division=0),
        "recall_pos": recall_score(y_test, y_pred, zero_division=0),
        "f1_pos": f1_score(y_test, y_pred, zero_division=0),
    }


# =====================================================================
# 4. MAIN SCRIPT
# =====================================================================
if __name__ == "__main__":

    # Load integrated multi-station dataset
    path = Path("colima_heavyrain_20mm_multistation.xlsx")
    df_raw = pd.read_excel(path)

    # Build all features
    df_feat = build_features(df_raw)

    # Export features (CSV = safe, universal)
    df_feat.to_csv("colima_heavyrain_features.csv", index=False)
    print("Feature dataset saved to colima_heavyrain_features.csv")
    print(f"Feature dataset shape: {df_feat.shape}")

    all_results = []

    # Train one model per horizon
    for target in ["HR_1d", "HR_2d", "HR_3d"]:
        print("\n" + "=" * 60)
        print(f"Training model for target: {target}")

        res = train_and_eval(df_feat, target)
        all_results.append(res)

        print(f"Positives in train: {res['positives_train']}")
        print(f"Positives in test:  {res['positives_test']}")
        print(f"Test size:          {res['test_size']}")
        print(f"Accuracy:           {res['accuracy']:.3f}")
        print(f"Precision (heavy):  {res['precision_pos']:.3f}")
        print(f"Recall (heavy):     {res['recall_pos']:.3f}")
        print(f"F1 (heavy):         {res['f1_pos']:.3f}")

    # Summary at end
    print("\nSUMMARY (all horizons)")
    for r in all_results:
        print(
            f"{r['target']}: acc={r['accuracy']:.3f}, "
            f"prec={r['precision_pos']:.3f}, "
            f"rec={r['recall_pos']:.3f}, "
            f"f1={r['f1_pos']:.3f}"
        )
