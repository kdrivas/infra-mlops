from cerberus import Validator
import fire
import yaml
import os
import pandas as pd
import logging

from model.utils.config import RAW_DIR, INTERM_DIR

logger = logging.getLogger(__name__)


def validate_assets(path: str, dry_run: bool = False) -> None:
    """Make sure that every input file follow the expected schema"""

    if dry_run:
        logger.info("Dry run activated - Skipping asset validation")
    else:
        logger.info("Dry run is not activated - Running validation")
        schema_path = "model/schema.yaml"

        with open(schema_path, "r") as stream:
            schemas = yaml.safe_load(stream)

        # Iterate over each schema and check if
        for name_schema in schemas:
            logger.debug(f"Validating schema for {name_schema}")
            validator = Validator()
            input_data_path = os.path.join(path, RAW_DIR, f"{name_schema}.csv")
            data = pd.read_csv(input_data_path)
            validator.validate(data.to_dict(orient="list"), schemas[name_schema])
            if validator.errors:
                exit(f"Validation failed: {validator.errors}")


if __name__ == "__main__":
    fire.Fire(validate_assets)
