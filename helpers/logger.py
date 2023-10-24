import logging
from rich.logging import RichHandler
from pathlib import Path
import os


# Create an configure the main logger
logger = logging.getLogger(__name__)


def configure(log_level: str = "INFO", 
              log_file: Path = None):  # type: ignore
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    log_level_value = getattr(logging, 
                              log_level.upper(), 
                              logging.INFO)

    logging.basicConfig(
        level=log_level_value,
        format=log_format,
        datefmt="[%X]",
        handlers=[RichHandler(rich_tracebacks=True)],
        )

    if log_file:
        # Create a log directory if it doesn't exist  
        log_dir = "logs"
        log_dir.mkdir(exist_ok=True)

        log_file_path = os.path.join(log_dir, log_file)

        file_handler = logging.FileHandler(log_file_path)
        file_handler.setFormatter(logging.Formatter(log_format))
        logger.addHandler(file_handler)
        logger.info(f"Log file: {log_file}")

    logger.info(f"Logger set up with log level: {log_level_value}({log_level})")
   
