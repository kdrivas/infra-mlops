import os
import joblib
import pandas as pd
import logging
import fire
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split

from model.utils.constants import CITY_COLS, PIB_COLS, IMACEC_INDICE_COLS, TAKE_VARS
from model.utils.constants import TARGET_COL

from model.utils.config import ARTIFACT_DIR, INTERM_DIR, FEATURE_DIR, MERGED_FILE_NAME

from model.utils.data_munging import (
    FixingFormattedString,
    TakeVariables,
    RollingTransformer,
)

logger = logging.getLogger(__name__)


def feature_engineering(path: str, dry_run: bool = False) -> None:
    """
    This function will execute the data preprocessing and serialize the data pipeline
    """

    logger.info("=======================================================")
    if dry_run:
        logger.info("Dry run activated - Running feature engineering")
    else:
        logger.info("Dry run is not activated - Running feature engineering")
    logger.info("=======================================================")

    # The pipeline is divided in two parts due to the presence of null values
    pipe = Pipeline(
        [
            ("fixing_pib_vars", FixingFormattedString(PIB_COLS, "PIB")),
            (
                "fixing_imacec_indice_vars",
                FixingFormattedString(IMACEC_INDICE_COLS, "IMACEC_INDICE"),
            ),
            (
                "rolling_with_mean",
                RollingTransformer(
                    ["Precio_leche"] + CITY_COLS + PIB_COLS + IMACEC_INDICE_COLS, "mean"
                ),
            ),
            (
                "rolling_with_std",
                RollingTransformer(
                    ["Precio_leche"] + CITY_COLS + PIB_COLS + IMACEC_INDICE_COLS, "std"
                ),
            ),
            ("take_vars_before_scaler", TakeVariables(TAKE_VARS)),
        ]
    )
    logger.info(f"The current pipeline is:\n {pipe}")

    # Read the data and set the period as index
    df_merge = pd.read_csv(os.path.join(path, INTERM_DIR, f"{MERGED_FILE_NAME}.csv"))
    df_merge["Periodo"] = df_merge.apply(lambda x: str(int(x.anio)) + "-" + str(int(x.mes)), axis=1)
    df_merge.set_index("Periodo", inplace=True)

    # Apply the first step of the preprocessing and remove nan
    logger.debug("Applying fit_transform to features")
    df_prec = pipe.fit_transform(df_merge.drop(TARGET_COL, axis=1), df_merge[TARGET_COL])
    df_interm = pd.concat((df_merge[TARGET_COL], df_prec), axis=1)
    df_interm = df_interm.dropna()

    logger.debug("Splitting data")
    (df_interm_X_train, df_interm_X_test, df_interm_y_train, df_interm_y_test,) = train_test_split(
        df_interm.drop([TARGET_COL], axis=1),
        df_interm[TARGET_COL],
        test_size=0.2,
        random_state=42,
    )
    df_prec_train = pd.concat((df_interm_y_train, pd.DataFrame(df_interm_X_train)), axis=1)
    df_prec_test = pd.concat((df_interm_y_test, pd.DataFrame(df_interm_X_test)), axis=1)

    if not dry_run:
        logger.info("Saving features")
        df_prec_train.to_csv(os.path.join(path, FEATURE_DIR, "train.csv"), index=False)
        df_prec_test.to_csv(os.path.join(path, FEATURE_DIR, "test.csv"), index=False)

        logger.info("Saving data pipeline")
        joblib.dump(pipe, os.path.join(ARTIFACT_DIR, "data_pipeline.pkl"))


if __name__ == "__main__":
    fire.Fire(feature_engineering)
