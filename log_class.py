import logging
import time


class Logger:
    """Custom logging class to handle application logs."""

    def __init__(self, log_file="app.log"):
        """Initialize the logger with file and console handlers."""
        self.logger = logging.getLogger("AppLogger")
        self.logger.setLevel(logging.DEBUG)  # Set logging level to capture all logs

        # Prevent duplicate log handlers
        if not self.logger.handlers:
            # Create a file handler for logging
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(logging.DEBUG)

            # Create a console handler
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)

            # Define log format
            formatter = logging.Formatter(
                "%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
            )
            file_handler.setFormatter(formatter)
            console_handler.setFormatter(formatter)

            # Add handlers to logger
            self.logger.addHandler(file_handler)
            self.logger.addHandler(console_handler)

    def info(self, message):
        """Log an INFO message."""
        self.logger.info(message)

    def warning(self, message):
        """Log a WARNING message."""
        self.logger.warning(message)

    def error(self, message):
        """Log an ERROR message."""
        self.logger.error(message)

    def debug(self, message):
        """Log a DEBUG message."""
        self.logger.debug(message)

    def track_execution_time(self, function):
        """Decorator to log execution time of a function."""

        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = function(*args, **kwargs)
            elapsed_time = time.time() - start_time
            self.info(f"Executed {function.__name__} in {elapsed_time:.4f} seconds")
            return result
        return wrapper
