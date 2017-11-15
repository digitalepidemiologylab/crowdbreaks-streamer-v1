import logging
import logging.handlers
import os
import pathlib

class Logger:
    @staticmethod
    def setup(name, filename=None):
        """
        Sets up logging to an all.log file and filename if provided.

        """
        log_file = 'all.log'
        folder = 'logs'
        pathlib.Path(folder).mkdir(parents=True, exist_ok=True)
        logfile_path = os.path.join('.', folder, log_file)

        log_file_max_size = 1024 * 1024 * 20  # megabytes
        log_num_backups = 3
        log_format = "%(asctime)s [%(levelname)-5.5s] [%(name)-9.9s]: %(message)s"
        log_filemode = "w"  # w: overwrite; a: append
        logging.basicConfig(filename=logfile_path, format=log_format, filemode=log_filemode, level=logging.DEBUG)
        rotate_file = logging.handlers.RotatingFileHandler(
            logfile_path, maxBytes=log_file_max_size, backupCount=log_num_backups
        )

        # create logger
        logger = logging.getLogger(name)
        logger_handlers = []

        # Console output line.
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        log_formatter = logging.Formatter(log_format)
        console_handler.setFormatter(log_formatter)
        rotate_file.setFormatter(log_formatter)
        logger_handlers.extend([rotate_file, console_handler])

        # set specific output file only for this logger
        if filename is not None:
            logfile_path = os.path.join('.', folder, filename)

            rotate_file_specific = logging.handlers.RotatingFileHandler(
                logfile_path, mode='w', maxBytes=log_file_max_size, backupCount=log_num_backups
            )
            rotate_file_specific.setFormatter(log_formatter)
            logger_handlers.append(rotate_file_specific)

        logger.handlers = logger_handlers

        return logger

