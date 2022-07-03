import pandas as pd
import joblib
import logging
from typing import Dict

from fastapi import FastAPI, status

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(funcName)s - %(message)s",
    level=logging.DEBUG,
)

logger = logging.getLogger(__name__)

# Only load the artifacts the first time
logger.info("Loading artifacts")
model_pipe = joblib.load("/opt/artifacts/model_prod.pkl")
data_pipe = joblib.load("/opt/artifacts/data_pipeline.pkl")

app = FastAPI()


@app.get("/check_service", status_code=status.HTTP_201_CREATED)
def root() -> Dict:
    return {"Message": "Hello world from service"}


@app.post("/get_prediction", status_code=status.HTTP_201_CREATED)
async def send_online_data(payload: Dict) -> Dict:
    """
    Get the prediction for the requested data
    Input:
        data: The data from three periods before the period you want to predict
    """
    data = payload["data"]
    data = pd.DataFrame(data)

    logger.debug("Applying tranform")
    data_prec = data_pipe.transform(data)
    data_prec = data_prec.dropna()
    logger.debug("Making predictions")
    preds = model_pipe.predict(data_prec)

    return {"prediction": preds[-1]}
