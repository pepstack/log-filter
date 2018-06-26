#!/usr/bin/python
#-*- coding: UTF-8 -*-
# @file: ctls_log_filter.py
#    日志文件过滤处理程序
#
# @create: 2018-06-19
# @update: 2018-06-26 16:37:30
#
#######################################################################
import os, sys, stat, signal, shutil, inspect, commands, hashlib, time, datetime, yaml

import multiprocessing
from multiprocessing import Process, Queue, Manager
from Queue import Empty, Full

import optparse, ConfigParser

#######################################################################
# application specific
APPFILE = os.path.realpath(sys.argv[0])
APPHOME = os.path.dirname(APPFILE)
APPNAME,_ = os.path.splitext(os.path.basename(APPFILE))
APPVER = "2.0.2"
APPHELP = "log files filter and processing"

# import your local modules
import utils.utility as util
import utils.evntlog as elog

# process statistics
from handlers.process_stat import ProcessStat

#######################################################################
# 下面的参数可以更加需要更改:

ext_filter_dict = {
    "" : False,
    ".md5" : False,
    ".lock" : False,
    ".position" : False,
    ".entrydb" : False
}


start_filter_dict = {
    ":enabled" : False,
    "pt_login." : True
}


# 64 KB. 每次处理数据缓冲区大小. 必须大于 1 行的字节数!
CHUNK_SIZE = 65536

# 16 MB. 每次打开文件处理的最大字节
READ_MAXSIZE = 16777216

# 队列文件数
QUEUE_SIZE = 512

# 目录扫描间隔时间秒: >= 3
SWEEP_INTERVAL_SECONDS = 10

#######################################################################
# md5 字符串
def md5string(str):
    m = hashlib.md5()
    m.update(str)
    return m.hexdigest()


#######################################################################
# 用户定义的函数, 从列字段值中取得 logger_name. 如:
#   logger_name_colval = "180616-23:18:37 SceneServerExpLog[21] INFO: [Exp]"
#   logger_name = Exp
def findLoggerName(msgrow):
    nameColVal = msgrow[0]
    begin = nameColVal.rfind('[')
    end = nameColVal.rfind(']')
    return nameColVal[begin + 1 : end].strip()


# 实际的子进程处理日志文件的函数
def doWorker(pstat, log_handlers, done_queue, logkey, logfile, stopfile, positionfile):

    (messages, lastposition) = util.relay_read_messages(logfile, positionfile, stopfile, CHUNK_SIZE, READ_MAXSIZE)

    send_lines = 0

    for msgline in messages:
        msgcols = msgline.split('|')
        msgrow = []

        for col in msgcols:
            msgrow.append(col.strip(' '))

        try:
            # msgrow:
            #   180616-23:18:37 SceneServerExpLog[21] INFO: [Exp] | 2 | 2018-06-16 23:18:37 | 5182 | 1 | 0 | 0 | 1-530053258 | 103426 | Db21 | 1 | 完成任务-普通奖励 | 102-野兽的威胁 | 1 | 250 | 0 | 250 | 0 | 0 | 1 | 1 | 10 | 10 | 71002-孙悟空(少年) | 500-剑齿虎 | 73,172
            #   180616-23:19:06 SceneServerExpLog[21] INFO: [LevelUp] | 2 | 2018-06-16 23:19:06 | 5182 | 1 | 0 | 0 | 1-530053258 | 103426 | Db21 | 50 | 1 | 50 | 97 | 146 | 127 | 71002-孙悟空(少年) | 830-东部野外 | 36,153

            # 得到 loggerName
            loggerName = findLoggerName(msgrow)

            # 根据名称查找 logger 并设置当前输出的日志文件
            logger = log_handlers[loggerName]

            # 当前 logger 的配置
            loggerConfig = logger.config

            # 日期字段列
            split_time_col = loggerConfig['split_time_col']

            # 根据字段内的时间戳生成日志输出文件的名字
            fileTitle = util.name_by_split_minutes(msgrow[ split_time_col ],
                loggerConfig['split_minutes'],
                loggerConfig['file_prefix'],
                loggerConfig['file_suffix'])

            # 组合成真正的日志输出文件
            loggerFile = os.path.join(loggerConfig['path_prefix'], fileTitle)

            # 设置当前使用的日志输出文件
            logger.setlogfile(loggerFile)

            # 输出行日志内容
            rowline = "|".join(msgrow)

            # 输出行日志到日志输出文件
            logger.log(rowline)

            send_lines += 1

            # 打印日志内容
            elog.debug_clean(rowline)

            pass
        except IndexError as ie:
            elog.error("IndexError error: %s", msgline)
            pass
        except KeyError as ke:
            elog.error("KeyError error: %s", msgline)
            pass
        except Exception as ex:
            elog.error("Exception: %s", msgline)
            pass

    pstat.statistic(pname, "send-lines", send_lines)
    pstat.statistic(pname, "error-lines", len(messages) - send_lines)
    pass


#######################################################################
# 子进程 (worker) 过程
#   循环从任务队列 sweep_queue 取一个待处理的任务进行处理, 并将结果放入 done_queue,
#   直到遇到'STOP'
#
def handler_worker(pstat, sweep_queue, done_queue, dictLogfile, loghandlersDict, logger_dictConfig, stopfile):
    from handlers.pipe_logger import PipeLogger

    pname = multiprocessing.current_process().name

    pstat.start(pname)

    log_handlers = {
    }

    for loghandlerName, loghandlerConfig in loghandlersDict.items():
        try:
            elog.info("create pipe logger: %s", loghandlerName)

            log_handlers[loghandlerName] = PipeLogger(logger_name=loghandlerName,
                loghandler_config=loghandlerConfig,
                logger_config=logger_dictConfig,
                logfile=None)

        except Exception as ex:
            elog.fatal("failed to create pipe logger: %s. %r", loghandlerName, ex)
            os.mknod(stopfile)
            pstat.stop(pname)
            elog.error("%s stopped for config error.", pname)
            return
        pass

    if len(log_handlers) == 0:
        os.mknod(stopfile)
        pstat.stop(pname)
        elog.error("%s exit for log-handlers not found.", pname)
        return

    while not util.file_exists(stopfile):
        logkey = None

        try:
            # block=True, timeout=1s
            (logkey, logfile, positionfile) = sweep_queue.get(True, 1)

            elog.debug("sweep_queue get: %s => %s (%s)", logkey, logfile, positionfile)

            # 调用实际的处理函数
            doWorker(pstat, log_handlers, done_queue, logkey, logfile, stopfile, positionfile)

        except Empty as ee:
            pass
        except KeyError as ke:
            elog.error("App error: %r", ke)
            pass
        except Exception as ex:
            elog.error("Exception: %r", ex)
            pass
        finally:
            if logkey:
                # 处理完毕, 从全局字典中移除
                dlogfile, dposfile = dictLogfile.pop(logkey)
                elog.info("pop dict: %s => %s (%s)", logkey, dlogfile, dposfile)
            pass

    pstat.stop(pname)

    elog.warn("%s stopped. elapsed %d seconds", pname, pstat.elapsedSeconds(pname))


#######################################################################
# 实际执行的过滤日志文件的处理函数, 返回 (passed, positionfile)
#
def doFilter(logkey, logfile, filename, curtime, position_stash):
    positionfile = None

    try:
        # 首先是文件路径名的匹配
        title, ext = os.path.splitext(filename)

        if not ext_filter_dict.get(ext, True):
            return (False, positionfile)

        if start_filter_dict.get(":enabled", False):
            # TODO:
            pass

        # 比较文件的时间
        # TODO:

        # positionfile: 字节偏移文件的全路径名
        if position_stash is None:
            positionfile = logfile + ".position"
        else:
            positionfile = os.path.join(position_stash, logkey + ".position")

        if not util.file_exists(positionfile):
            util.write_first_line_nothrow(positionfile, "0")
            pass

        firstline = util.read_first_line_nothrow(positionfile)
        if firstline is None:
            elog.error("position file not found: %s", positionfile)
            return (False, positionfile)

        offset = int(firstline)
        if offset < 0:
            elog.fatal("bad position file: %s", positionfile)
            return (False, positionfile)

        filesize = util.file_size_nothrow(logfile)
        if offset < filesize:
            return (True, positionfile)

    except TypeError as te:
        elog.error("TypeError: %r", te)
        pass
    except Exception as ex:
        elog.error("Exception: %r", ex)
        pass

    return (False, positionfile)


#######################################################################
# 过滤日志文件, 符合处理要求返回 (True, logkey, positionfile)
#
def filter_logfile(logfile, filename, curtime, dictLogfile, position_stash):
    try:
        logkey = md5string(logfile)

        # 先判断是否正在处理中
        if dictLogfile.has_key(logkey):
            return (False, logkey, None)

        # 实际执行的过滤函数
        (passed, positionfile) = doFilter(logkey, logfile, filename, curtime, position_stash)

        if passed:
            # 需要处理的日志.
            #  setdefault 如果键不存在于字典中，将会添加键并将值设为默认值。
            #  如果键存在于字典中，不设置
            dictLogfile.setdefault(logkey, (logfile, positionfile))

            return (True, logkey, positionfile)
        else:
            # 不需要处理的日志
            return (False, logkey, positionfile)

    except Exception as ex:
        elog.error("Unexpected error: %r", ex)
        pass

    return (False, None, None)


def sweep_path(pstat, pname, sweep_queue, dictLogfile, path, curtime, stopfile, position_stash):
    files = os.listdir(path)
    files.sort(key=lambda x:x[0:20])

    for f in files:

        if util.file_exists(stopfile):
            break

        pf = os.path.join(path, f)

        if util.dir_exists(pf):
            sweep_path(pstat, pname, sweep_queue, dictLogfile, pf, curtime, stopfile, position_stash)
        elif util.file_exists(pf):
            passed, logkey, positionfile = filter_logfile(pf, f, curtime, dictLogfile, position_stash)

            if passed:
                try:
                    sweep_queue.put_nowait((logkey, pf, positionfile))
                    elog.info("sweep_queue put: %s => %s (%s)", logkey, pf, positionfile)
                    pstat.statistic(pname, "passed-files", 1)
                except Full:
                    elog.warn("sweep_queue if full. wait for %d seconds", SWEEP_INTERVAL_SECONDS)

                    for i in range(SWEEP_INTERVAL_SECONDS):
                        if util.file_exists(stopfile):
                            break
                        time.sleep(1)
                except:
                    elog.error("%r: %s", sys.exc_info(), pf)
                    break
            else:
                elog.debug("ignored file: %s", pf)
                pstat.statistic(pname, "ignored-files", 1)
                pass
    pass


def sweeper_worker(pstat, watch_paths, sweep_queue, dictLogfile, stopfile, position_stash):
    pname = multiprocessing.current_process().name

    pstat.start(pname)

    while not util.file_exists(stopfile):
        for path in watch_paths:
            if util.file_exists(stopfile):
                break

            elog.debug("sweep path: %s", path)

            try:
                sweep_path(pstat, pname, sweep_queue, dictLogfile, path, time.time(), stopfile, position_stash)
            except:
                elog.error("%r: %s", sys.exc_info(), path)
            finally:
                for i in range(SWEEP_INTERVAL_SECONDS):
                    if util.file_exists(stopfile):
                        break
                    time.sleep(1)
                pass

    pstat.stop(pname)

    elog.warn("%s stopped. elapsed %d seconds", pname, pstat.elapsedSeconds(pname))

    pass


#######################################################################
# main entry function
#
def main(parser, config):
    import utils.logger as logger

    (options, args) = parser.parse_args(args=None, values=None)

    logger_dictConfig = logger.set_logger(config['logger'], options.log_path, options.log_level)
    if not logger_dictConfig:
        elog.error("logger.config error: %s", config['logger']['logging_config'])
        print "**** logger.config error:", config['logger']['logging_config']
        sys.exit(-1)

    stopfile = config['stopfile']
    if options.forcestop:
        elog.warn("create stop file: %s", stopfile)
        os.mknod(stopfile)
        sys.exit(0)

    # 监控文件的路径
    #
    watch_paths = options.watch_paths
    if watch_paths is None and config.has_key('watch-paths'):
        watch_paths = config['watch-paths']
    watch_paths = util.parse_pathstr(watch_paths)

    # 位置文件保存路径
    #   如果为 None, 则与日志文件目录相同
    position_stash_path = options.position_stash
    if position_stash_path is None and config.has_key('position-stash'):
        position_stash_path = config['position-stash']

    # 路径如果指定则必须存在
    if not position_stash_path is None:
        position_stash_path = os.path.realpath(position_stash_path)
        if not util.dir_exists(position_stash_path):
            elog.error("position stash path not existed: %s", position_stash_path)
            sys.exit(-1)

    # 进程数
    #   = [1, cpus]
    num_handlers = options.num_workers
    if num_handlers < 1:
        elog.warn("number of workers(=%d) is too less. force it with 1", num_handlers)
        num_handlers = 1
        pass

    if num_handlers > multiprocessing.cpu_count():
        elog.warn("number of workers(=%s) is too many. force it with %d", num_handlers, multiprocessing.cpu_count())
        num_handlers = multiprocessing.cpu_count()
        pass

    # 取得 log-handlers 配置文件的内容
    log_handlers_config = None
    fd = None
    try:
        abs_config_file = os.path.realpath(options.config_file)
        fd = open(abs_config_file)
        log_handlers_config = yaml.load(fd)['log-handlers']
        elog.info("using log-handlers config file: %s", abs_config_file)
    except:
        elog.warn("using default config in file: %s", APPFILE)
        log_handlers_config = config['log-handlers']
        pass
    finally:
        util.close_file_nothrow(fd)
        pass

    if options.check_config:
        try:
            # 对于每个 log-handlers 是否存在 logger
            loggersDict = logger_dictConfig['loggers']
            handlersDict = logger_dictConfig['handlers']

            for hdlName, _ in log_handlers_config.items():
                if len(loggersDict[hdlName]['handlers']) == 0:
                    elog.error("loggers: %s has no handlers: [] (file: %s)", hdlName,
                        config['logger']['logging_config'])
                    break

                for hdler in loggersDict[hdlName]['handlers']:
                    hdlCfg = handlersDict[hdler]

                    elog.force_clean("    '%s': %r", hdler, hdlCfg)
        except KeyError as ke:
            elog.error("check config failed: %r", ke)
            pass
        sys.exit(0)

    # 启动服务
    elog.force("%s-%s startup", APPNAME, APPVER)

    elog.force("watch paths        : %s", watch_paths)
    elog.force("position stash     : %r", position_stash_path)
    elog.force("sweep queue size   : %d", config['sweep-queue-size'])
    elog.force("done queue size    : %d", config['done-queue-size'])
    elog.force("number of workers  : %d", num_handlers)
    elog.force("force stop file    : %s", stopfile)

    # 全局共享字典, 防止文件重复处理
    # https://stackoverflow.com/questions/6832554/python-multiprocessing-how-do-i-share-a-dict-among-multiple-processes
    dictLogfile = Manager().dict()

    # 创建任务队列
    elog.info("create sweep queue")
    sweep_queue = Queue(config['sweep-queue-size'])

    # 创建任务完成队列: 当前未使用
    elog.info("create done queue")
    done_queue = Queue(config['done-queue-size'])

    # 每分钟(60秒)打印一次统计报告
    pstat = ProcessStat(reportInterval = 60)

    # 创建单个 sweep 进程: 创建任务放入任务队列
    sweep_proc = Process(target = sweeper_worker, args = (pstat, watch_paths, sweep_queue, dictLogfile, stopfile, position_stash_path))

    sweepProcNameList = [ sweep_proc.name ]

    # 创建多个 handler 进程: 从任务取出任务队列并执行
    p_handlers = []
    workerProcNameList = []
    for i in range(num_handlers):
        p = Process(target = handler_worker, args = (pstat, sweep_queue, done_queue, dictLogfile,
                log_handlers_config, logger_dictConfig, stopfile))
        p.daemon = True
        p.start()
        p_handlers.append(p)

        workerProcNameList.append(p.name)

        # wait for 0.1 seconds
        time.sleep(0.1)
        pass

    # 启动扫描目录单进程
    sweep_proc.daemon = True
    sweep_proc.start()

    # 永远运行并间歇打印统计报告
    cnt = 0
    while not util.file_exists(stopfile):
        cnt += 1

        time.sleep(1)

        if cnt == pstat.reportInterval:
            cnt = 0
            pstat.printReport(sweepProcNameList, "SWEEP")
            pstat.printReport(workerProcNameList, "FILTER")
        pass

    # block wait child process exit
    sweep_proc.join()

    # block wait child processes exit
    for p in p_handlers:
        p.join()

    # 全部服务中止
    elog.fatal("%s-%s shutdown.", APPNAME, APPVER)

    pstat.printReport(sweepProcNameList, "SWEEP")
    pstat.printReport(workerProcNameList, "FILTER")

    pass


#######################################################################
# Usage:
#
#   $ %prog -N 20 -C ./config.yaml -L ERROR
#
#
if __name__ == "__main__":
    parser, group, optparse = util.use_parser_group(APPNAME, APPVER, APPHELP,
        '%prog [Options]')

    group.add_option("-O", "--log-path",
        action="store", dest="log_path", type="string", default="/var/log/applog",
        help="指定程序日志路径 (不是要监控的日志)",
        metavar="LOGPATH")

    group.add_option("-L", "--log-level",
        action="store", dest="log_level", type="string", default="DEBUG",
        help="指定程序日志水平: DEBUG, WARN, INFO, ERROR. default: DEBUG",
        metavar="LOGLEVEL")

    group.add_option("-W", "--watch-paths",
        action="store", dest="watch_paths", type="string", default=None,
        help="指定监控的文件目录. 默认为空. 此参数必须指定!",
        metavar="PATHS")

    group.add_option("-P", "--position-stash",
        action="store", dest="position_stash", type="string", default=None,
        help="指定处理过程中的字节偏移文件保存的目录. 默认为监控文件所在的目录. 此目录不允许删除.",
        metavar="PATH")

    group.add_option("-C", "--handlers-config",
        action="store", dest="config_file", type="string", default=None,
        help="指定配置文件 (yaml 格式). 默认为不指定, 使用默认配置.",
        metavar="CFGPATH")

    group.add_option("-N", "--num-workers",
        action="store", dest="num_workers", type="int", default=1,
        help="指定处理进程数目. 默认 1.",
        metavar="NUM")

    group.add_option("--check-config",
        action="store_true", dest="check_config", default=False,
        help="不启动程序, 仅仅检查配置文件是否正确")

    group.add_option("--forcestop",
        action="store_true", dest="forcestop", default=False,
        help="安全地中止本程序的所有进程服务")

    # 如果 STOP 文件存在, 则终止服务. 程序启动时自动删除这个文件
    stopfile = os.path.join(APPHOME, APPNAME + ".FORCESTOP")
    util.remove_file_nothrow(stopfile)

    # 下面缺省的配置会覆盖参数化的配置中的默认为空(None)的配置
    #
    defaultConfig = {
        'sweep-queue-size' : QUEUE_SIZE,
        'done-queue-size' : QUEUE_SIZE,
        'watch-paths' : './stashcsv',
        'position-stash' : '/var/log/position-stash',
        'stopfile' : stopfile,
        'logger' : {
            'logging_config': os.path.join(APPHOME, 'conf/logger.config'),
            'file': APPNAME + '.log',
            'name': 'main'
        },
        'log-handlers' : {
            "Exp" : {
                "path_prefix" : "/var/log/output",
                "file_prefix" : "exp_",
                "file_suffix" : ".log",
                "split_time_col" : 2,
                "split_minutes" : 5
            },
            "LevelUp" : {
                "path_prefix" : "/var/log/output",
                "file_prefix" : "levelup_",
                "file_suffix" : ".log",
                "split_time_col" : 2,
                "split_minutes" : 5
            },
            "LevelUp2" : {
                "path_prefix" : "/var/log/output",
                "file_prefix" : "levelup2_",
                "file_suffix" : ".log",
                "split_time_col" : 2,
                "split_minutes" : 5
            }
        }
    }

    main(parser, defaultConfig)

    sys.exit(0)
