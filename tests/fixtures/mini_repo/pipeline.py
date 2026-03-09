import pandas as pd

import helpers

SOURCE_PATH = "data/orders.csv"
TARGET_PATH = "data/orders_clean.csv"


class PipelineJob(BaseJob):
    pass


def _build_orders() -> None:
    df = pd.read_csv(SOURCE_PATH)
    cleaned = helpers.transform_orders(df)
    cleaned.to_csv(TARGET_PATH)
