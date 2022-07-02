import os
import logging
import pandas as pd
import fire

from model.utils.constants import MERGE_COLS, MILK_COLS, BANK_COLS, PREP_COLS
from model.utils.constants import TARGET_COL

from model.utils.config import MILK_FILE_NAME, PREP_FILE_NAME, BANK_FILE_NAME
from model.utils.config import MERGED_FILE_NAME
from model.utils.config import RAW_DIR, INTERM_DIR


logger = logging.getLogger(__name__)


def collect_milk_data(path: str, dry_run: bool = False) -> None:
    """
    This function will collect the data and create columns for the merge step
    """
    df = pd.read_csv(os.path.join(path, RAW_DIR, f"{MILK_FILE_NAME}.csv"))

    df.rename(columns={"Anio": "anio", "Mes": "mes"}, inplace=True)
    df["mes"] = df["mes"].replace(
        {
            "Ene": 1,
            "Feb": 2,
            "Mar": 3,
            "Abr": 4,
            "May": 5,
            "Jun": 6,
            "Jul": 7,
            "Ago": 8,
            "Sep": 9,
            "Oct": 10,
            "Nov": 11,
            "Dic": 12,
        }
    )

    if dry_run:
        logger.info("Skipping saving")
    else:
        logger.info("Saving milk data")
        df[MILK_COLS].to_csv(
            os.path.join(path, INTERM_DIR, f"collect_{MILK_FILE_NAME}.csv"), index=False
        )


def collect_prep_data(path: str, dry_run: bool = False) -> None:
    """
    This function will collect the data and create columns for the merge step
    """
    df = pd.read_csv(os.path.join(path, RAW_DIR, f"{PREP_FILE_NAME}.csv"))

    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
    df[["mes", "anio"]] = df["date"].apply(lambda x: pd.Series([x.month, x.year]))

    if dry_run:
        logger.info("Skipping saving")
    else:
        logger.info("Saving prep data")
        df[PREP_COLS].to_csv(
            os.path.join(path, INTERM_DIR, f"collect_{PREP_FILE_NAME}.csv"), index=False
        )


def collect_bank_data(path: str, dry_run: bool = False) -> None:
    """
        This function will collect the data and create columns for the merge step
    """
    df = pd.read_csv(os.path.join(path, RAW_DIR, f"{BANK_FILE_NAME}.csv"))

    df["Periodo"] = pd.to_datetime(
        df["Periodo"], infer_datetime_format=True, errors="coerce"
    )

    original_size = len(df)
    df = df.drop_duplicates(subset="Periodo")
    logger.debug(f"Dropping duplicated {original_size - df.shape[0]} records")
    df[["anio", "mes"]] = df["Periodo"].apply(lambda x: pd.Series([x.year, x.month]))

    original_shape = df.shape
    df = df[BANK_COLS].dropna()
    logger.debug(
        f"The df was reduced from {original_shape} to {df.shape} after dropna operation"
    )

    if dry_run:
        logger.info("Skipping saving")
    else:
        logger.info("Saving bank data")
        df[BANK_COLS].to_csv(
            os.path.join(path, INTERM_DIR, f"collect_{BANK_FILE_NAME}.csv"), index=False
        )


def merge_data(path: str, dry_run: bool = False) -> None:
    """
    This function will merge the data from 3 sources. Using the path, the function
    will read the data sources from the previous step
    """
    df_bank = pd.read_csv(
        os.path.join(path, INTERM_DIR, f"collect_{MILK_FILE_NAME}.csv")
    )
    df_milk = pd.read_csv(
        os.path.join(path, INTERM_DIR, f"collect_{PREP_FILE_NAME}.csv")
    )
    df_prec = pd.read_csv(
        os.path.join(path, INTERM_DIR, f"collect_{BANK_FILE_NAME}.csv")
    )

    df_merge = pd.merge(df_milk, df_prec, on=["mes", "anio"], how="inner")
    df_merge = pd.merge(df_merge, df_bank, on=["mes", "anio"], how="inner")
    df_merge = df_merge.sort_values(by=["anio", "mes"], ascending=True).reset_index(
        drop=True
    )

    # Shift variables
    # Shift operations won"t be part of the production pipeline
    df_merge[TARGET_COL] = df_merge["Precio_leche"]
    df_merge[MERGE_COLS] = df_merge[MERGE_COLS].shift(1)

    if dry_run:
        logger.info("Skipping saving")
    else:
        logger.info("Saving merging data")
        df_merge[MERGE_COLS + [TARGET_COL]].dropna().to_csv(
            os.path.join(path, INTERM_DIR, f"{MERGED_FILE_NAME}.csv"), index=False
        )


def preprocess_assets(path: str, dry_run: bool = False) -> None:
    """
    Preprocess the data and create an intermidiate table which will be used in 
    feature engineering
    """

    if dry_run:
        logger.info("Dry run activated - Running preprocessing")
    else:
        logger.info("Dry run is not activated - Running preprocessing")

    logger.debug("Starting preprocessing with milk data")
    collect_milk_data(path, dry_run)

    logger.debug("Starting preprocessing with prep data")
    collect_prep_data(path, dry_run)

    logger.debug("Starting preprocessing with bank data")
    collect_bank_data(path, dry_run)

    logger.debug("Creating intermediate data")
    merge_data(path)


if __name__ == "__main__":
    fire.Fire(preprocess_assets)
