# To allow running as a script
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from prefect import flow, get_run_logger

from helpers.alert_handlers import failure_handler
from models import PreMongoPerson
from processors.person_processor import person_processor
from transformers.jockey_ratings_transformer import transform_jockey_ratings


@flow(on_failure=[lambda flow, flow_run, state: failure_handler("Flow", flow.name, state)])
def load_jockey_ratings():
    logger = get_run_logger()
    logger.info("Starting jockey rating loader")

    p = person_processor()
    next(p)

    for jockey, rating in transform_jockey_ratings().items():
        ratings = {k: v for k, v in rating.items() if v}
        p.send((PreMongoPerson(name=jockey, role="jockey", ratings=ratings), "rr"))

    p.close()


if __name__ == "__main__":
    load_jockey_ratings()
