#!/usr/bin/python
#-*- coding: UTF-8 -*-
#
# @file: pipe_logger.py
#
#    logging message.
#
# @author: zhangliang@ztgame.com
#
# @create: 2018-06-20 11:23:58
#
# @update: 2018-06-21 15:45:08
#
#######################################################################
import os, sys, time, yaml

# logging.config.dictConfig requires python2.7
import logging, logging.config


import common

import utils.utility as util
import utils.evntlog as elog

#######################################################################

class PipeLogger(object):
    def __init__(self, **kwargs):
        self.logger_name = kwargs['logger_name']

        self.config = kwargs['loghandler_config']

        self.dictcfg = kwargs['logger_config']

        self.init(kwargs['logfile'])

        pass


    def __del__(self):
        self.cleanup()
        pass


    def cleanup(self):
        removehandlers = []
        for handler in self.logger.handlers:
            removehandlers.append(handler)

        for handler in removehandlers:
            if handler.stream:
                elog.debug("close stream: %r", self.logfile)
                handler.stream.close()
            self.logger.removeHandler(handler)
        pass


    def init(self, logfile = None):
        # update dictcfg with logfile
        if logfile:
            elog.update_log_config(self.dictcfg, self.logger_name, logfile, 'INFO')

        # reload config
        logging.config.dictConfig(self.dictcfg)

        # update logger
        self.logger = logging.getLogger(self.logger_name)
        self.logfile = logfile
        pass


    def setlogfile(self, logfile):
        if self.logfile != logfile:
            ###elog.debug("using: %r", logfile)
            self.cleanup()
            self.init(logfile)


    def log(self, message):
        # use critical to ensure alway output message
        self.logger.critical(message)
        pass
