import logging

from hpcflow import RUN_TIME_INFO
from hpcflow.utils import sentry_wrap

__all__ = ("make_workflow",)

logger = logging.getLogger(__name__)


def make_workflow(dir: str):
    """Make a new workflow, innit.

    Parameters
    ----------
    dir
        Directory to make new workflow in.
    """
    with sentry_wrap("make_workflow") as span:
        logger.info(f"make_workflow; is_venv: {RUN_TIME_INFO.is_venv}")
