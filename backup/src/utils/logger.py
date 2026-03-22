"""Logging utilities for the pipeline."""

import logging


def get_logger(name: str) -> logging.Logger:
    """Return a configured logger.

    Args:
        name: Logger name (typically __name__ of the calling module).

    Returns:
        Configured Logger instance.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
        )
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger
