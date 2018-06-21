#!/usr/bin/python
#-*- coding: UTF-8 -*-
#
# @filename: logger.py
# 2017-12-08
################################################################################

def set_logger(logger, log_path, log_level):
    import os, sys
    import utility as util
    import evntlog as elog

    loggingConfigDict = {}

    # set logger:
    logpath = util.source_abspath(log_path)
    if not os.path.isdir(logpath):
        elog.error("log path not found: %r", logpath)
        sys.exit(1)

    logging_config = logger['logging_config']
    if not util.file_exists(logging_config):
        elog.error("logging.config file not found: %s", logging_config)
        sys.exit(1)

    # init logger: main
    try:
        logger_file = os.path.join(logpath, logger['file'])

        configDict = elog.init_logger(
            logger_name = logger['name'],
            logging_config = logging_config,
            logger_file = logger_file,
            logger_level = log_level)

        elog.force("logging config: %s", logging_config)
        elog.force("logger file: %s", logger_file)
        elog.force("logger level: %s", log_level)
        elog.force("logger name: %s", logger['name'])

        return configDict
    except Exception as ex:
        elog.error("error init logger: %r", ex)
        sys.exit(1)
