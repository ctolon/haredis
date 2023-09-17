import logging
import sys


# Logging formatter supporting colorized output
class _LogFormatter(logging.Formatter):
    COLOR_CODES = {
        logging.CRITICAL: "\033[1;35m",  # bright/bold magenta
        logging.ERROR: "\033[1;31m",  # bright/bold red
        logging.WARNING: "\033[1;33m",  # bright/bold yellow
        logging.INFO: "\033[0;37m",  # white / light gray
        logging.DEBUG: "\033[1;30m"  # bright/bold black / dark gray
    }

    RESET_CODE = "\033[0m"

    def __init__(self, color, *args, **kwargs):
        super(_LogFormatter, self).__init__(*args, **kwargs)
        self.color = color

    def format(self, record, *args, **kwargs):
        if (self.color == True and record.levelno in self.COLOR_CODES):
            record.color_on = self.COLOR_CODES[record.levelno]
            record.color_off = self.RESET_CODE
        else:
            record.color_on = ""
            record.color_off = ""
        return super(_LogFormatter, self).format(record, *args, **kwargs)


loggers = {}


# Set up logging
def set_up_logging(
        log_to_console=True,
        log_to_file=True,
        console_log_output="stdout",
        console_log_level="INFO",
        console_log_color=True,
        logfile_file="redis.log",
        logfile_log_level="INFO",
        logfile_log_color=False,
        log_line_template="%(color_on)s[%(asctime)s] [%(levelname)s] %(message)s%(color_off)s",
        logger_name="redis",
        log_propagate=False
) -> logging.Logger:
    """Set up logging function.

    Args:
        log_to_console (bool, optional): If True log to console. Defaults to True.
        log_to_file (bool, optional): If True log to file. Defaults to True.
        console_log_output (str, optional): Type of console log output. Defaults to "stdout".
        console_log_level (str, optional): console log level. Defaults to "INFO".
        console_log_color (bool, optional): If True colorized console log output. Defaults to True.
        logfile_file (str, optional): Path to log file with name. Defaults to "redis.log".
        logfile_log_level (str, optional): logfile log level. Defaults to "INFO".
        logfile_log_color (bool, optional): If True colorized log file output. Defaults to False.
        log_line_template (str, optional): Log Line template. Defaults to "%(color_on)s[%(asctime)s] [%(levelname)s] %(message)s%(color_off)s".
        logger_name (str, optional): Name of the logger instance. Defaults to "redis".
        log_propagate (bool, optional): If True propagate to logger. Defaults to False.

    Returns:
        logging.Logger: A Logger instance.
    """
    global loggers

    if loggers.get(logger_name):
        return loggers.get(logger_name)
    else:
        logger = logging.getLogger(logger_name)
    logger.propagate = log_propagate

    # Set global log level to 'debug' (required for handler levels to work)
    logger.setLevel(logging.DEBUG)

    # Create console handler
    console_log_output = console_log_output.lower()
    if (console_log_output == "stdout"):
        console_log_output = sys.stdout
    elif (console_log_output == "stderr"):
        console_log_output = sys.stderr
    else:
        print("Failed to set console output: invalid output: '%s'" % console_log_output)
        return False
    console_handler = logging.StreamHandler(console_log_output)

    # Set console log level
    try:
        console_handler.setLevel(console_log_level.upper())  # only accepts uppercase level names
    except:
        print("Failed to set console log level: invalid level: '%s'" % console_log_level)
        return False

    # Create and set formatter, add console handler to logger
    if log_to_console:
        console_formatter = _LogFormatter(fmt=log_line_template, datefmt="%Y/%m/%d %H:%M:%S", color=console_log_color)
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

    # Create log file handler
    try:
        logfile_handler = logging.FileHandler(logfile_file)
    except Exception as exception:
        print("Failed to set up log file: %s" % str(exception))
        return False

    # Set log file log level
    try:
        logfile_handler.setLevel(logfile_log_level.upper())  # only accepts uppercase level names
    except:
        print("Failed to set log file log level: invalid level: '%s'" % logfile_log_level)
        return False

    # Create and set formatter, add log file handler to logger
    if log_to_file:
        logfile_formatter = _LogFormatter(fmt=log_line_template, color=logfile_log_color)
        logfile_handler.setFormatter(logfile_formatter)
        logger.addHandler(logfile_handler)

    # Add logger to global loggers dict
    loggers[logger_name] = logger

    # Success
    return logger
