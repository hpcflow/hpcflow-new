import logging

from hpcflow import RUN_TIME_INFO

__all__ = ("make_workflow",)

logger = logging.getLogger(__name__)


def make_workflow(dir: str):
    """Make a new workflow, innit.

    Parameters
    ----------
    dir
        Directory to make new workflow in.
    """
    logger.info(f"make_workflow; is_venv: {RUN_TIME_INFO.is_venv}")
