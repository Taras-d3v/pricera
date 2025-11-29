import logging
import logging.config
import yaml
import os

logger = logging.getLogger("logger_init")


def set_logger():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(current_dir, "logging.yaml")

    try:
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
        logging.config.dictConfig(config)
        logger.info("Logger configuration loaded successfully")
    except FileNotFoundError:
        logger.error("Failed to load logging configuration, using basic logger instead")
        logging.basicConfig(level=logging.DEBUG)
