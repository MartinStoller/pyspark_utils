import logging
import sys


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)

    if not logger.handlers:  # Prevent duplicate handlers in Glue/notebooks
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            fmt="%(asctime)s — %(levelname)s — %(name)s — %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

    logger.propagate = False  # Prevent duplicate logs if root logger is configured
    return logger


def get_df_preview(df, n=5) -> str:
    """
    previewing function, which prevents materializing the full DataFrame.
    Developed mostly for Glue and local setup. Databricks or Jupyter Notebooks probably dont need this.
    """
    try:
        return df._jdf.showString(n, 20, False)
    except Exception as e:
        return f"[DataFrame preview failed: {e}]"
