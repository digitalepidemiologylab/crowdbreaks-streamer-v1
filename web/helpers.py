import rollbar

def report_error(logger, msg, level='error'):
    if level == 'error':
        logger.error(msg)
    elif level == 'warning':
        logger.warning(msg)
    rollbar.report_message(msg, level)
