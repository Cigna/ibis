"""Logging module"""
import logging


logger = None


def get_logger(cfg_mgr):
    """Return logger"""
    global logger

    # LOGGER
    if logger is not None:
        return logger

    log_format = ('[%(asctime)-10s: %(filename)21s:%(lineno)4s'
                  ' - %(funcName)20s() %(levelname)7s] %(message)s')
    formatter = logging.Formatter(log_format)
    logger = logging.getLogger('ibis_logger')
    hdlr = logging.FileHandler(cfg_mgr.log_file)
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.INFO)
    return logger
