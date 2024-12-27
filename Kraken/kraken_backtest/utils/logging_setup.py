import logging
import os

def configure_logging(log_file_name="kraken_backtest.log", log_level=logging.INFO):
    """
    Configure logging for the application.

    Args:
        log_file_name (str): Name of the log file.
        log_level (int): Logging level, e.g., logging.INFO or logging.DEBUG.
    """
    # Create the logs directory if it doesn't exist
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Define the full path for the log file
    log_file_path = os.path.join(log_dir, log_file_name)

    # Define a logging format
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    # Set up logging handlers
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(log_level)
    file_handler.setFormatter(logging.Formatter(log_format))

    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(logging.Formatter(log_format))

    # Configure the root logger
    logging.basicConfig(level=log_level, handlers=[file_handler, console_handler])

    # Log an initialization message
    logging.info("Logging is configured. Logs will be written to: %s", log_file_path)
