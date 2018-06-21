#!/usr/bin/python
#-*- coding: UTF-8 -*-
#
# evntlog.py
#   python event log functions
#
# * ------ color output ------
# * 30: black
# * 31: red
# * 32: green
# * 33: yellow
# * 34: blue
# * 35: purple
# * 36: dark green
# * 37: white
# * --------------------------
# * "\033[31m RED   \033[0m"
# * "\033[32m GREEN \033[0m"
# * "\033[33m YELLOW \033[0m"
#
# init created: 2015-12-22
# last updated: 2017-12-08
#######################################################################
import os, sys, inspect, time, datetime, types, logging

reload(sys)
sys.setdefaultencoding('utf-8')

#######################################################################
# public functions
import logging, logging.config

# you can modify below 3 lines:
#
pid_enabled = True
default_logger_name = 'main'
default_logger_level = 'DEBUG'


def nowtime():
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))


def get_level():
    level = logger.level
    if not level:
        return 'UNSET'
    if level == 10:
        return 'DEBUG'
    elif level == 20:
        return 'INFO'
    elif level == 30:
        return 'WARN'
    elif level == 40:
        return 'ERROR'
    elif level == 50:
        return 'FATAL'
    else:
        return 'LEVEL#' + str(level)


def set_level(loglevel, set_pid_enabled = True):
    global pid_enabled
    level = ["DEBUG", "INFO", "WARN", "ERROR", "FATAL"].index(loglevel)
    logger.setLevel( (logging.DEBUG, logging.INFO, logging.WARN, logging.ERROR, logging.FATAL)[level] )
    pid_enabled = set_pid_enabled
    pass


def force_clean(format = None, *arg):
    if format is None:
        msg = ''
    else:
        msg = format % arg
    logger.critical(msg)


def force(format = None, *arg):
    global pid_enabled
    caller = inspect.currentframe().f_back
    func = caller.f_code.co_name
    filename = os.path.basename(caller.f_code.co_filename)
    lineno = caller.f_lineno
    if format is None:
        msg = ''
    else:
        msg = format % arg
    if pid_enabled:
        event = "[%s - %s:%d - %s] <%d:FORCE> %s" % (nowtime(), filename, lineno, func, os.getpid(), msg)
    else:
        event = "[%s - %s:%d - %s] <FORCE> %s" % (nowtime(), filename, lineno, func, msg)
    msg = '\033[36m%s\033[0m' % (event)
    logger.critical(msg)


def debug_clean(format = None, *arg):
    if logger.level <= logging.DEBUG:
        if format is None:
            msg = ''
        else:
            msg = format % arg
        logger.debug(msg)


def debug(format = None, *arg):
    global pid_enabled
    if logger.level <= logging.DEBUG:
        caller = inspect.currentframe().f_back
        func = caller.f_code.co_name
        filename = os.path.basename(caller.f_code.co_filename)
        lineno = caller.f_lineno
        if format is None:
            msg = ''
        else:
            msg = format % arg
        if pid_enabled:
            event = "[%s - %s:%d - %s] <%d:DEBUG> %s" % (nowtime(), filename, lineno, func, os.getpid(), msg)
        else:
            event = "[%s - %s:%d - %s] <DEBUG> %s" % (nowtime(), filename, lineno, func, msg)
        logger.debug(event)


def info(format = None, *arg):
    global pid_enabled
    if logger.level <= logging.INFO:
        caller = inspect.currentframe().f_back
        func = caller.f_code.co_name
        filename = os.path.basename(caller.f_code.co_filename)
        lineno = caller.f_lineno
        if format is None:
            msg = ''
        else:
            msg = format % arg
        if pid_enabled:
            event = "[%s - %s:%d - %s] <%d:INFO> %s" % (nowtime(), filename, lineno, func, os.getpid(), msg)
        else:
            event = "[%s - %s:%d - %s] <INFO> %s" % (nowtime(), filename, lineno, func, msg)
        msg = '\033[32m%s\033[0m' % (event)
        logger.info(msg)


def warn(format = None, *arg):
    global pid_enabled
    if logger.level <= logging.WARN:
        caller = inspect.currentframe().f_back
        func = caller.f_code.co_name
        filename = os.path.basename(caller.f_code.co_filename)
        lineno = caller.f_lineno
        if format is None:
            msg = ''
        else:
            msg = format % arg
        if pid_enabled:
            event = "[%s - %s:%d - %s] <%d:WARN> %s" % (nowtime(), filename, lineno, func, os.getpid(), msg)
        else:
            event = "[%s - %s:%d - %s] <WARN> %s" % (nowtime(), filename, lineno, func, msg)
        msg = '\033[33m%s\033[0m' % (event)
        logger.warn(msg)


def error(format = None, *arg):
    global pid_enabled
    if logger.level <= logging.ERROR:
        caller = inspect.currentframe().f_back
        func = caller.f_code.co_name
        filename = os.path.basename(caller.f_code.co_filename)
        lineno = caller.f_lineno
        if format is None:
            msg = ''
        else:
            msg = format % arg
        if pid_enabled:
            event = "[%s - %s:%d - %s] <%d:ERROR> %s" % (nowtime(), filename, lineno, func, os.getpid(), msg)
        else:
            event = "[%s - %s:%d - %s] <ERROR> %s" % (nowtime(), filename, lineno, func, msg)
        msg = '\033[31m%s\033[0m' % (event)
        logger.error(msg)


def fatal(format = None, *arg):
    global pid_enabled
    if logger.level <= logging.CRITICAL:
        caller = inspect.currentframe().f_back
        func = caller.f_code.co_name
        filename = os.path.basename(caller.f_code.co_filename)
        lineno = caller.f_lineno
        if format is None:
            msg = ''
        else:
            msg = format % arg
        if pid_enabled:
            event = "[%s - %s:%d - %s] <%d:FATAL> %s" % (nowtime(), filename, lineno, func, os.getpid(), msg)
        else:
            event = "[%s - %s:%d - %s] <FATAL> %s" % (nowtime(), filename, lineno, func, msg)
        msg = '\033[35m%s\033[0m' % (event)
        logger.critical(msg)


#######################################################################
# init logger

logging.basicConfig(stream=sys.stdout, format='%(message)s', level=logging.DEBUG)
logger = logging.getLogger()


def update_log_config(dictcfg, logger_name, logger_file, logger_level):
    try:
        dictcfg['loggers'][logger_name]['level'] = logger_level

        for h in dictcfg['loggers'][logger_name]['handlers']:
            # update log file
            if logger_file:
                dictcfg['handlers'][h]['filename'] = logger_file

            # update level
            dictcfg['handlers'][h]['level'] = logger_level

        # update root level if propagate is True
        if dictcfg['loggers'][logger_name]['propagate']:
            dictcfg['root']['level'] = logger_level
            for h in dictcfg['root']['handlers']:
                dictcfg['handlers'][h]['level'] = logger_level
    except:
        print "error update_log_config"
        pass


def init_logger(**kwargs):
    global logger

    fd = None
    try:
        import yaml

        logger_name = kwargs.get('logger_name')
        if not logger_name:
            logger_name = default_logger_name

        logger_level = kwargs.get('logger_level')
        if not logger_level:
            logger_level = default_logger_level

        logging_conf = kwargs.get('logging_config')

        if logging_conf and os.path.exists(logging_conf) and os.path.isfile(logging_conf):
            config_yaml = logging_conf
        else:
            p = os.path.split(inspect.getfile( inspect.currentframe() ))[0]
            d = os.path.realpath(os.path.abspath(p))
            config_yaml = os.path.join(d, "logging.config")

        info("load logging.config: %s", config_yaml)

        fd = open(config_yaml)
        dictcfg = yaml.load(fd)
        logger_file = kwargs.get('logger_file')

        update_log_config(dictcfg, logger_name, logger_file, logger_level)

        try:
            logging.config.dictConfig(dictcfg)
            logger = logging.getLogger(logger_name)
        except Exception as e:
            debug("using basicConfig for: %r", ex)
            logging.basicConfig(stream=sys.stdout, format='%(message)s', level=logging.DEBUG)
            logger = logging.getLogger()
        finally:
            return dictcfg

    except Exception as ex:
        debug("using basicConfig for: %r", ex)
        logging.basicConfig(stream=sys.stdout, format='%(message)s', level=logging.DEBUG)
        logger = logging.getLogger()
        return None
    finally:
        if fd:
            fd.close()
