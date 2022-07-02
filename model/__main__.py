from typing import Callable, Dict

import fire
import logging
import logging.config

from model.steps.feature_engineering import feature_engineering
from model.steps.validation import validate_assets
from model.steps.preprocessing import preprocess_assets

tasks: Dict[str, Callable] = {
    "validate_assets": validate_assets,
    "preprocess_assets": preprocess_assets,
    "feature_engineering": feature_engineering,
}


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(funcName)s - %(message)s",
        level=logging.DEBUG,
    )

    fire.Fire(tasks)
