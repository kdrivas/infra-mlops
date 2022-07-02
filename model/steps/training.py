"""
    This file contains functions for model training and validation
"""
import os
import joblib
from datetime import datetime
import json
import shutil
import pandas as pd
import logging
from sklearn.model_selection import GridSearchCV
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.pipeline import Pipeline

from sklearn.preprocessing import PolynomialFeatures, StandardScaler
from sklearn.feature_selection import SelectKBest, mutual_info_regression

from model.utils.constants import TARGET_COL, PARAM_GRID
from model.utils.config import ARTIFACT_DIR, FEATURE_DIR

logger = logging.getLogger(__name__)


def hypertune_model(path: str, dry_run: bool = False) -> None:
    """
    This function will train the model using the preprocessed data (train and test sets)
    Once the model is trained, the metrics and the serialized model is stored in the artifact_path
    """
    if dry_run:
        logger.info("Dry run activated - Running hypertune")
    else:
        logger.info("Dry run is not activated - Running hypertune")

    train = pd.read_csv(os.path.join(path, FEATURE_DIR, "train.csv"))

    pipe = Pipeline(
        [
            # Moving the standardScaler to the data processing pipeline causes problems of reproducibility
            ("scale", StandardScaler()),
            ("selector", SelectKBest(mutual_info_regression)),
            ("poly", PolynomialFeatures()),
            ("model", Ridge()),
        ]
    )

    X_train, y_train = train.drop(TARGET_COL, axis=1), train[TARGET_COL]

    grid = GridSearchCV(estimator=pipe, param_grid=PARAM_GRID, cv=3, scoring="r2")
    logger.info("Fitting the model")
    grid.fit(X_train, y_train)
    best_params = grid.best_params_

    logger.info("Best params:", best_params)
    if dry_run:
        logger.info("Skipping saving")
    else:
        logger.info("Saving best parameters")
        with open(os.path.join(ARTIFACT_DIR, "best_params.json"), "w") as f:
            json.dump(best_params, f, indent=4)


def training_model(path: str, dry_run: bool = False) -> None:
    """
    This function will train the model using the preprocessed data (train and test sets)
    Once the model is trained, the metrics and the serialized model is stored in the artifact_path
    """
    if dry_run:
        logger.info("Dry run activated - Running training")
    else:
        logger.info("Dry run is not activated - Running training")

    train = pd.read_csv(os.path.join(path, FEATURE_DIR, "train.csv"))
    test = pd.read_csv(os.path.join(path, FEATURE_DIR, "test.csv"))

    with open(os.path.join(ARTIFACT_DIR, "best_params.json")) as f:
        params = json.load(f)

    # Prediction pipeline
    pipe = Pipeline(
        [
            # Moving the standardScaler to the data processing pipeline causes problems
            # of reproducibility
            ("scale", StandardScaler()),
            ("selector", SelectKBest(mutual_info_regression, k=params["selector__k"])),
            ("poly", PolynomialFeatures(degree=params["poly__degree"])),
            ("model", Ridge(alpha=params["model__alpha"])),
        ]
    )

    X_train, y_train = train.drop(TARGET_COL, axis=1), train[TARGET_COL]
    X_test, y_test = test.drop(TARGET_COL, axis=1), test[TARGET_COL]

    pipe.fit(X_train, y_train)

    y_pred = pipe.predict(X_test)

    rmse = mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    logger.info("RMSE:", rmse)
    logger.info("r2:", r2)

    metrics = {
        "RMSE": rmse,
        "r2": r2,
    }
    if dry_run:
        logger.info("Skipping saving")
    else:
        logger.info("Saving best parameters")
        # If there's a previous trained model, first save the previous version
        if os.path.exists(os.path.join(ARTIFACT_DIR, "model/model_metrics.json")):
            history_artifacts_dir = os.path.join(ARTIFACT_DIR, "model/history", datetime.now())
            os.makedirs(history_artifacts_dir)
            shutil.copyfile(
                os.path.join(ARTIFACT_DIR, "model/model_metrics.json"), history_artifacts_dir
            )
            shutil.copyfile(
                os.path.join(ARTIFACT_DIR, "model/model_staging.pkl"), history_artifacts_dir
            )

        with open(os.path.join(ARTIFACT_DIR, "model/model_metrics.json"), "w") as f:
            json.dump(metrics, f, indent=4)

        joblib.dump(pipe, os.path.join(ARTIFACT_DIR, "model/model_staging.pkl"))


def validate_promote_model(artifact_path: str) -> None:
    """
    Implements a set of validation for the model to promote it
    """
    with open(os.path.join(artifact_path, "model_metrics.json")) as json_file:
        metrics = json.load(json_file)

    model = joblib.load(os.path.join(artifact_path, "model_staging.pkl"))

    # Dummy validation for RMSE and R2
    assert metrics["RMSE"] > 0 and metrics["r2"] > 0

    # TO-DO: Validate the last model perform better than the previous model
    print("Everythin Ok :D")

    joblib.dump(model, os.path.join(artifact_path, "model_prod.pkl"))


def batch_prediction_model(artifact_path: str, data_path: str) -> None:
    """
    Predict batch data
    """

    # Read historic features from our feature store
    features = pd.read_csv(
        os.path.join(data_path, "feature_store", "super_efficient_feature_store.csv")
    )

    model = joblib.load(os.path.join(artifact_path, "model_prod.pkl"))

    pred = model.predict(features.drop("Periodo", axis=1))
    features["preds"] = pred

    features[["Periodo", "preds"]].to_csv(
        os.path.join(data_path, "batch_predictions", "predictions.csv"), index=False
    )
