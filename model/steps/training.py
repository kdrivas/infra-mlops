"""
    This file contains functions for model training and validation
"""
import os
import joblib
from datetime import datetime
import json
import fire
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


def hypertune_model(base_path: str, dry_run: bool = False) -> None:
    """
    This function will train the model using the preprocessed data (train and test sets)
    Once the model is trained, the metrics and the serialized model is stored in the artifact_path
    """
    logger.info("=======================================================")
    if dry_run:
        logger.info("Dry run activated - Running hypertune")
    else:
        logger.info("Dry run is not activated - Running hypertune")
    logger.info("=======================================================")

    train = pd.read_csv(os.path.join(base_path, FEATURE_DIR, "train.csv"))

    pipe = Pipeline(
        [
            # Moving the standardScaler to the data processing pipeline causes problems of reproducibility
            ("scale", StandardScaler()),
            ("selector", SelectKBest(mutual_info_regression)),
            ("poly", PolynomialFeatures()),
            ("model", Ridge()),
        ]
    )
    logger.info(f"The current pipeline is:\n {pipe}")

    X_train, y_train = train.drop(TARGET_COL, axis=1), train[TARGET_COL]

    grid = GridSearchCV(estimator=pipe, param_grid=PARAM_GRID, cv=3, scoring="r2")

    logger.info("Fitting the model")
    grid.fit(X_train, y_train)
    best_params = grid.best_params_
    logger.info(f"Best params: {best_params}")

    if dry_run:
        logger.info("Skipping saving")
    else:
        logger.info("Saving best parameters")
        with open(os.path.join(base_path, ARTIFACT_DIR, "params/best_params.json"), "w") as f:
            json.dump(best_params, f, indent=4)


def training_model(base_path: str, dry_run: bool = False) -> None:
    """
    This function will train the model using the preprocessed data (train and test sets)
    Once the model is trained, the metrics and the serialized model is stored in the artifact_path
    """
    logger.info("=======================================================")
    if dry_run:
        logger.info("Dry run activated - Running training")
    else:
        logger.info("Dry run is not activated - Running training")
    logger.info("=======================================================")

    train = pd.read_csv(os.path.join(base_path, FEATURE_DIR, "train.csv"))
    test = pd.read_csv(os.path.join(base_path, FEATURE_DIR, "test.csv"))

    with open(os.path.join(base_path, ARTIFACT_DIR, "params/best_params.json")) as f:
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
    logger.info(f"The current pipeline is:\n {pipe}")

    X_train, y_train = train.drop(TARGET_COL, axis=1), train[TARGET_COL]
    X_test, y_test = test.drop(TARGET_COL, axis=1), test[TARGET_COL]

    logger.info("Fitting the model")
    pipe.fit(X_train, y_train)

    y_pred = pipe.predict(X_test)

    rmse = mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    metrics = {
        "RMSE": rmse,
        "r2": r2,
    }

    logger.info(f"metrics: {metrics}")

    if dry_run:
        logger.info("Skipping saving")
    else:
        logger.info("Saving best parameters")

        # If there's a previous trained model, first save the previous version
        if os.path.exists(os.path.join(base_path, ARTIFACT_DIR, "model/model_metrics.json")):
            now = datetime.now()
            history_artifacts_dir = os.path.join(
                base_path, ARTIFACT_DIR, "model/history", now.strftime("%m-%d-%Y_%H:%M:%S")
            )
            os.makedirs(history_artifacts_dir)
            shutil.copyfile(
                os.path.join(base_path, ARTIFACT_DIR, "model/model_metrics.json"),
                os.path.join(history_artifacts_dir, "model_metrics.json"),
            )
            shutil.copyfile(
                os.path.join(base_path, ARTIFACT_DIR, "model/trained_model.pkl"),
                os.path.join(history_artifacts_dir, "trained_model.json"),
            )

        with open(os.path.join(base_path, ARTIFACT_DIR, "model/model_metrics.json"), "w") as f:
            json.dump(metrics, f, indent=4)

        joblib.dump(pipe, os.path.join(base_path, ARTIFACT_DIR, "model/trained_model.pkl"))


if __name__ == "__main__":
    fire.Fire(training_model)
